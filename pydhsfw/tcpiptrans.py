import threading
import time
import logging
import socket
import errno
from urllib.parse import urlparse
from collections import deque
from pydhsfw.threads import AbortableThread
from pydhsfw.messages import MessageIn, MessageOut, MessageFactory
from pydhsfw.connection import Connection
from pydhsfw.transport import Transport, TransportStream, TransportState, StreamReader, StreamWriter, MessageReader, MessageWriter

_logger = logging.getLogger(__name__)

WORKER_ABORT_CHECK_INTERVAL = 'worker_abort_check_interval'

class SocketStreamReader(StreamReader):
    def __init__(self, config:dict={}):
        self._sock = None
        self._read_timeout = config.get(WORKER_ABORT_CHECK_INTERVAL, 5.0)
        self._connected_event = threading.Event()

    @property
    def socket(self)->socket:
        return self._sock

    @socket.setter
    def socket(self, sock:socket):
        self._sock = sock
        if not self._sock:
            self._connected = False

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

        except OSError as e:
            if e.errno == errno.EBADF:
                #Socket has been closed, probably from this side for some reason. Convert to ConnectionAbortedError.
                raise ConnectionAbortedError("socket connection broken")
            else:
                raise e
        except Exception:
            # Log an exception here this way we can track other potential socket errors and 
            # handle them specifcally like above.
            _logger.exception()
            raise

    @property
    def _connected(self, is_connected:bool):
        if is_connected:
            self._connected_event.set()
        else:
            self._connected_event.clear()

class SocketStreamWriter(StreamWriter):
    def __init__(self):
        self._sock = None

    @property
    def socket(self, sock:socket):
        self._sock = sock

    def write(self, buffer:bytes):
        try:
            self._sock.sendall(buffer)
        except Exception:
            _logger.exception()
            raise


class TcpipTransport(TransportStream):
    ''' Tcpip transport base'''

    def __init(self, message_reader:MessageReader, message_writer:MessageWriter, config:dict={}):
        super().__init__(message_reader, message_writer, config)
        self._stream_reader = SocketStreamReader(config)
        self._stream_writer = SocketStreamWriter(config)

class TcpipClientTransportConnectionWorker(AbortableThread):

    def __init__(self, socket_stream_reader:SocketStreamReader, socket_stream_writer:SocketStreamWriter, config:dict={}):
        super().__init__(name='tcpip client transport connection worker', config=config)
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
        self._set_desired_state(TransportState.RECONNECTED)

    def disconnect(self):
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

                except TimeoutError:
                    # This is normal and allows for handling the SystemExit exception.
                    pass
                except Exception:
                    # Send all other exceptions to the log so we can analyse them to determine if
                    # they need special handling or possibly ignoring them.
                    _logger.exception()
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
        return self._config.get('url')

    def _set_state(self, state:TransportState):
        self._state = state
        msg = f'Connection state: {state}, url: {self._get_url()}'
        _logger.info(msg)

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

                end_time = time.time() + connect_timeout
                end_delay_time = time.time()

                while self._state_desired == TransportState.CONNECTED and (connect_timeout == None or time.time() < end_time):
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
                        if self._state_desired == TransportState.CONNECTED:
                            _logger.info(f'Connection timeout: cannot connect to {url}, trying again in {connect_retry_delay} seconds')
                            end_delay_time = time.time() + connect_retry_delay
                    except ConnectionRefusedError:
                        if self._state_desired == TransportState.CONNECTED:
                            _logger.info(f'Connection refused: cannot connect to {url}, trying again in {connect_retry_delay} seconds')
                            end_delay_time = time.time() + connect_retry_delay
                    except Exception:
                        _logger.exception()
                        self._set_state(TransportState.DISCONNECTED)
                        raise
            else:
                _logger.debug('Already connected, ignoring connection request')

    def _disconnect(self):

        if self._desired_state == TransportState.DISCONNECTED:
            url = self._get_url()
            # Only disconnect if we are connected
            if self.state == TransportState.CONNECTED:
                self._set_state(TransportState.DISCONNECTING)
                sock = self._stream_reader.socket
                self._stream_reader.socket = None
                self._stream_writer.socket = None
                if sock:
                    sock.shutdown(socket.SHUT_RDWR)
                    sock.close()
                self._set_state(TransportState.DISCONNECTED)
            else:
                _logger.debug("Not connected, ignoring disconnect request")

    def _reconnect(self):
        if self._desired_state == TransportState.RECONNECTED:
            self._desired_state == TransportState.DISCONNECTED
            self._disconnect()
            time.sleep(3)
            self._desired_state == TransportState.CONNECTED
            self._connect()

class TcpipClientTransport(TcpipTransport):
    ''' Tcpip client transport '''

    def __init(self, message_reader:MessageReader, message_writer:MessageWriter, config:dict=None):
        super().__init__(message_reader, message_writer, config)
        self._connection_worker = TcpipClientTransportConnectionWorker(self._stream_reader, self._stream_writer, config)

    def start(self):
        self._connection_worker.start()

    def shutdown(self):
        self._connection_worker.abort()

    def wait(self):
        self._connection_worker.wait()


class MessageQueue():

    def __init__(self):
        pass
    def _queque_message(self, message:MessageIn):
        pass
    def _get_message(self, timeout=None):
        pass
    def _clear_messages(self):
        pass

class BlockingMessageQueue(MessageQueue):

    def __init__(self):
        super().__init__()
        self._deque_message = deque()
        self._deque_event = threading.Event()

    def _queque_message(self, message:MessageIn):
        #Append message and unblock
        self._deque_message.append(message)
        self._deque_event.set()

    def _get_message(self, timeout=None):
        
        msg = None

        #Block until items are available
        if not self._deque_event.wait(timeout):
            raise TimeoutError
        
        elif self._deque_message: 
            msg = self._deque_message.popleft()

        #If there are no more items, start blocking again
        if not self._deque_message:
            self._deque_event.clear()
        return msg

    def _clear_messages(self):
            self._deque_event.clear()
            self._deque_message.clear()


class ConnectionReadWorker(AbortableThread):
    
    def __init__(self, transport:Transport, incoming_message_queue:MessageQueue, message_factory:MessageFactory, config:dict={}):
        super().__init__(name='connection read worker', config=config)
        self._transport = transport
        self._msg_queue = incoming_message_queue
        self._msg_factory = message_factory

    def run(self):

        # Only one thing to do here, read raw messages from the transport and translate them 
        # into messages using the message factory, then stick them on the incoming queue.

        try:
            while True:
                try:
                    # Blocking call with timeout configured elsewhere. This will timeout to check for control messages, specifically SystemExit.
                    raw_msg = self._transport.receive()
                    if raw_msg:
                        _logger.debug(f'Received unpacked raw message, len: {len(raw_msg)}, buffer: {raw_msg}')
                        msg = self._msg_factory.create_message(raw_msg)
                        if msg:
                            _logger.debug(f'Received factory created message: {msg}')
                            self._msg_queue._queque_message(msg)

                except TimeoutError:
                    #Socket read timed out. This is normal, it just means that no messages have been sent so we can ignore it.
                    pass
                except Exception:
                    # Log other exceptions so we can monitor and create special handlng if needed.
                    _logger.exception()
                    raise

        except SystemExit:
            _logger.info(f'Shutdown signal received, exiting {self.name}')
            
        finally:
            pass

class ConnectionWriteWorker(AbortableThread):
    
    def __init__(self, transport:Transport, outgoing_message_queue:MessageQueue, config:dict={}):
        super().__init__(name='connection reader worker', config=config)
        self._transport = transport
        self._msg_queue = outgoing_message_queue

    def run(self):

        # Only one thing to do here, read messages from the queue and write raw message to the transport

        try:
            while True:
                try:
                    # Blocking call with timeout configured elsewhere. This will timeout to check for control messages, specifically SystemExit.
                    msg = self._msg_queue._get_message(self._get_blocking_timeout())
                    if msg:
                        _logger.info(f"Sending message: {msg}")
                        buffer = msg.write()
                        _logger.info(f"Sending unpacked raw message: {buffer}")
                        self._transport.send(buffer)

                except TimeoutError:
                    #Socket read timed out. This is normal, it just means that no messages have been sent so we can ignore it.
                    pass
                except Exception:
                    # Log other exceptions so we can monitor and create special handlng if needed.
                    _logger.exception()
                    raise

        except SystemExit:
            _logger.info(f'Shutdown signal received, exiting {self.name}')
            
        finally:
            pass

class ConnectionBase(Connection):
    def __init__(self, transport:Transport, incoming_message_queue:MessageQueue, outgoing_message_queue:MessageQueue, message_factory:MessageFactory, config:dict={}):
        super().__init__(None, config)
        self._transport = transport
        self._read_worker = ConnectionReadWorker(transport, incoming_message_queue, message_factory, config)
        self._write_worker = ConnectionWriteWorker(transport, outgoing_message_queue, config)
        self._outgoing_message_queue = outgoing_message_queue
        self._transport.start()
        self._read_worker.start()
        self._write_worker.start()

    def connect(self):
        self._transport.connect()

    def disconnect(self):
        self._transport.disconnect()

    def send(self, msg:MessageOut):
        self._outgoing_message_queue._queque_message(msg)

    def shutdown(self):
        self._read_worker.abort()
        self._write_worker.abort()
        self._transport.abort()

    def wait(self):
        self._read_worker.wait()
        self._write_worker.wait()
        self._transport.wait()

