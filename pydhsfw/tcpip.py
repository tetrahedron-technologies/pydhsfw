import threading
import time
import logging
import socket
import errno
from urllib.parse import urlparse
from pydhsfw.threads import AbortableThread
from pydhsfw.transport import TransportStream, TransportState, StreamReader, StreamWriter, MessageStreamReader, MessageStreamWriter

_logger = logging.getLogger(__name__)

class SocketStreamReader(StreamReader):
    def __init__(self, config:dict={}):
        self._sock = None
        self._read_timeout = config.get(AbortableThread.THREAD_BLOCKING_TIMEOUT, AbortableThread.THREAD_BLOCKING_TIMEOUT_DEFAULT)
        self._connected_event = threading.Event()

    @property
    def socket(self):
        return self._sock

    @socket.setter
    def socket(self, sock:socket):
        self._sock = sock
        self._connected = bool(sock != None)

    def read(self, msglen:int)->bytes:

        try:
            # Wait for the connection to be established.
            if not self._connected_event.wait(self._read_timeout):
                raise TimeoutError()

            res = None

            if msglen:
                chunks = []
                bytes_recd = 0
                while bytes_recd < msglen:
                    chunk = self._sock.recv(min(msglen - bytes_recd, 2048))
                    if chunk == b'':
                        raise ConnectionAbortedError("socket connection broken")
                    chunks.extend(chunk)
                    chunk_len = len(chunk)
                    bytes_recd = bytes_recd + chunk_len

                res = bytearray(chunks)
            
            return res

        except socket.timeout:
            raise TimeoutError()

        except OSError as e:
            if e.errno == errno.EBADF:
                #Socket has been closed, probably from this side for some reason. Convert to ConnectionAbortedError.
                raise ConnectionAbortedError("socket connection broken")
            else:
                raise e
        except Exception:
            # Log an exception here this way we can track other potential socket errors and 
            # handle them specifcally like above.
            _logger.exception(None)
            raise

    @property
    def _connected(self):
        raise NotImplementedError
    
    @_connected.setter
    def _connected(self, is_connected:bool):
        if is_connected:
            self._connected_event.set()
        else:
            self._connected_event.clear()

class SocketStreamWriter(StreamWriter):
    def __init__(self, config:dict={}):
        self._sock = None

    @property
    def socket(self):
        return self._sock

    @socket.setter
    def socket(self, sock:socket):
        self._sock = sock

    def write(self, buffer:bytes):
        try:
            self._sock.sendall(buffer)
        except Exception:
            _logger.exception(None)
            raise


class TcpipTransport(TransportStream):
    ''' Tcpip transport base'''

    def __init__(self, connection_name:str, url:str,  message_reader:MessageStreamReader, message_writer:MessageStreamWriter, config:dict={}):
        super().__init__(connection_name, url, message_reader, message_writer, config)
        self._stream_reader = SocketStreamReader(config)
        self._stream_writer = SocketStreamWriter(config)

class TcpipClientTransportConnectionWorker(AbortableThread):

    def __init__(self, connection_name:str, url:str, socket_stream_reader:SocketStreamReader, socket_stream_writer:SocketStreamWriter, config:dict={}):
        super().__init__(name=f'{connection_name} tcpip client transport connection worker', config=config)
        self._connection_name = connection_name
        self._url = url
        self._config = config
        self._stream_reader = socket_stream_reader
        self._stream_reader._read_timeout = self._get_blocking_timeout()
        self._stream_writer = socket_stream_writer
        self._state = TransportState.DISCONNECTED
        self._desired_state = TransportState.DISCONNECTED
        self._state_change_event = threading.Event()

    def connect(self):
        self._set_desired_state(TransportState.CONNECTED)

    def reconnect(self):
        self._stream_reader._connected = False
        self._set_desired_state(TransportState.RECONNECTED)

    def disconnect(self):
        self._stream_reader._connected = False
        self._set_desired_state(TransportState.DISCONNECTED)

    def _set_desired_state(self, state:TransportState):
        if self._desired_state != state:
            self._desired_state = state
            self._state_change_event.set()

    def run(self):

        # This loop does three things.
        # 1. Monitor the desired state and compare it to the actual state. If it's different call the 
        # appropriate method to modify the state.
        # 2. Pop out of any blocking calls to test for a SystemExit exception so the thread can be
        # shutdown cleanly.
        # 3. Possibly send a heartbeat or keepalive to the server.

        try:
            while True:
                try:
                    if not self._state_change_event.wait(self._get_blocking_timeout()):
                        # Can't wait forever in blocking call, need to enter loop to check for control messages, specifically SystemExit.
                        raise TimeoutError

                    if self._desired_state == TransportState.CONNECTED:
                        self._connect()
                    elif self._desired_state == TransportState.DISCONNECTED:
                        self._disconnect()
                    elif self._desired_state == TransportState.RECONNECTED:
                        self._reconnect()

                    self._state_change_event.clear()

                except TimeoutError:
                    # This is normal and allows for handling the SystemExit exception.
                    pass
                except Exception:
                    # Send all other exceptions to the log so we can analyse them to determine if
                    # they need special handling or possibly ignoring them.
                    _logger.exception(None)
                    raise

        except SystemExit:
            _logger.info(f'Shutdown signal received, exiting {self.name}')
        finally:
            try:
                self._disconnect()
            except:
                pass

    @property
    def state(self):
        return self._state
      
    def _get_url(self):
        return self._url

    def _set_state(self, state:TransportState):
        self._state = state
        _logger.info(f'Connection state: {state}, url: {self._get_url()}')
        if state in (TransportState.CONNECTED, TransportState.DISCONNECTED):
            #TODO[Giles]: Add ConnectionConnectedMessage or ConnectionDisconnectedMessage to the queue.
            pass


    def _connect(self):

        if self._desired_state == TransportState.CONNECTED:

            if self.state == TransportState.DISCONNECTED:

                socket_timeout = self._get_blocking_timeout()
                connect_timeout = self._config.get('connect_timeout', None)
                connect_retry_delay = self._config.get('connect_retry_delay', 10)
                url = self._get_url()
                uparts = urlparse(url)

                sock = None

                self._set_state(TransportState.CONNECTING)

                end_time = time.time() + float(connect_timeout or 0.0)
                end_delay_time = time.time()

                while self._desired_state == TransportState.CONNECTED and (connect_timeout == None or time.time() < end_time):
                    try:
                        if time.time() >= end_delay_time:
                            sock = socket.socket()
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                            sock.settimeout(socket_timeout)
                            sock.connect((uparts.hostname, uparts.port))
                            self._stream_reader.socket = sock
                            self._stream_writer.socket = sock
                            self._set_state(TransportState.CONNECTED)
                            break
                        else:
                            time.sleep(socket_timeout)

                    except socket.timeout:
                        if self._desired_state == TransportState.CONNECTED:
                            _logger.info(f'Connection timeout: cannot connect to {url}, trying again in {connect_retry_delay} seconds')
                            end_delay_time = time.time() + connect_retry_delay
                    except ConnectionRefusedError:
                        if self._desired_state == TransportState.CONNECTED:
                            _logger.info(f'Connection refused: cannot connect to {url}, trying again in {connect_retry_delay} seconds')
                            end_delay_time = time.time() + connect_retry_delay
                    except Exception:
                        _logger.exception(None)
                        self._set_state(TransportState.DISCONNECTED)
                        raise
            else:
                _logger.debug('Already connected, ignoring connection request')

    def _disconnect(self):

        if self._desired_state == TransportState.DISCONNECTED:
            # Only disconnect if we are connected
            if self.state == TransportState.CONNECTED:
                self._set_state(TransportState.DISCONNECTING)
                sock = self._stream_reader.socket
                self._stream_reader.socket = None
                self._stream_writer.socket = None
                if sock:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except:
                        _logger.warning('No socket available to shutdown')
                    finally:
                        sock.close()
                self._set_state(TransportState.DISCONNECTED)
            else:
                _logger.debug("Not connected, ignoring disconnect request")

    def _reconnect(self):
        
        if self._desired_state == TransportState.RECONNECTED:
            self._desired_state = TransportState.DISCONNECTED
            self._state = TransportState.CONNECTED
            self._disconnect()
            time.sleep(self._get_blocking_timeout())
            self._desired_state = TransportState.CONNECTED
            self._connect()

class TcpipClientTransport(TcpipTransport):
    ''' Tcpip client transport '''

    def __init__(self, connection_name:str, url:str, message_reader:MessageStreamReader, message_writer:MessageStreamWriter, config:dict={}):
        super().__init__(connection_name, url, message_reader, message_writer, config)
        self._connection_worker = TcpipClientTransportConnectionWorker(connection_name, url, self._stream_reader, self._stream_writer, config)

    def connect(self):
        self._connection_worker.connect()

    def disconnect(self):
        self._connection_worker.disconnect()

    def reconnect(self):
        self._connection_worker.reconnect()

    def start(self):
        self._connection_worker.start()

    def shutdown(self):
        self._connection_worker.abort()

    def wait(self):
        self._connection_worker.join()

