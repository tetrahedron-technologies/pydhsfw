import threading
import socket
import time
import errno
import logging
from traceback import print_exc
from collections import deque
from urllib.parse import urlparse
from pydhsfw.threads import AbortableThread
from pydhsfw.messages import MessageIn, MessageOut, MessageFactory
from pydhsfw.connection import Connection, MessageReader, MessageProcessor, ClientState

_logger = logging.getLogger(__name__)

class TcpipSocketReader(MessageReader):

    def read_socket(self, sock:socket):
        pass

    def _read(self, sock:socket, msglen:int=None, delimeter:bytes=None)->bytes:
        res = None

        if msglen:
            chunks = []
            bytes_recd = 0
            while bytes_recd < msglen:
                chunk = sock.recv(min(msglen - bytes_recd, 2048))
                if chunk == b'':
                    raise ConnectionAbortedError("socket connection broken")
                chunks.extend(chunk)
                chunk_len = len(chunk)
                bytes_recd = bytes_recd + chunk_len

            res = bytearray(chunks)

        elif delimeter:
            #TODO[Giles]: Implement this to pull bytes until the delimeter is found, then return the full message chunk.
            pass

        return res

    def read_to_delimiter(self, sock:socket, delimeter:bytes)->bytes:
        pass

class TcpipConnection(Connection):
    pass

class TcpipClientConnectionWorker(AbortableThread):
    
    class ConnectControlMessage(MessageOut):
        def __init__(self, config:dict):
            self._cfg = config
        def get_config(self):
            return self._cfg

    class ReconnectControlMessage(MessageOut):
        def __init__(self, config:dict=None):
            self._cfg = config
        def get_config(self):
            return self._cfg

    class DisconnectControlMessage(MessageOut):
        pass

    def __init__(self):
        super().__init__(name='tcpip client connection worker')
        self._config = {}

        self._control_message = None
        self._deque_message = deque()
        self._deque_event = threading.Event()
        
        self._state = ClientState.DISCONNECTED
        self._state_desired = ClientState.DISCONNECTED

        self._sock = None
        self._sock_event = threading.Event()

    def send_message(self, message:MessageOut):
        
        #Append message and unblock
        self._deque_message.append(message)
        self._deque_event.set()

    def connect(self, config:dict):
        self._state_desired = ClientState.CONNECTED

        #If connected or connecting and the url is different then reconnect.
        if self.get_state() in (ClientState.CONNECTED, ClientState.CONNECTING):
            if config.get('url') != self._get_url():
                self._state_desired = ClientState.DISCONNECTED
                self._send_control_message(TcpipClientConnectionWorker.ReconnectControlMessage(config))
        else:
            self._send_control_message(TcpipClientConnectionWorker.ConnectControlMessage(config))

    def reconnect(self, config:dict=None):
        self._state_desired = ClientState.CONNECTED
        self._send_control_message(TcpipClientConnectionWorker.ReconnectControlMessage(config))

    def disconnect(self):
        self._state_desired = ClientState.DISCONNECTED

        if self.get_state() in (ClientState.CONNECTED, ClientState.CONNECTING):
            self._send_control_message(TcpipClientConnectionWorker.DisconnectControlMessage())


    def run(self):
        sock = None

        try:
            while True:
                try:
                    #Can't wait forever in blocking call, need to enter loop to check for control messages, specifically SystemExit.
                    msg = self._get_message(5)
                    if msg:
                        if isinstance(msg, TcpipClientConnectionWorker.ConnectControlMessage):
                            sock = self._connect(msg.get_config())
                            self._sock = sock
                            self._sock_event.set()    

                        elif isinstance(msg, TcpipClientConnectionWorker.ReconnectControlMessage):
                            self._sock_event.clear()    
                            sock = self._reconnect(sock, msg.get_config())
                            _sock = sock
                            self._sock_event.set()    
                        
                        elif isinstance(msg, TcpipClientConnectionWorker.DisconnectControlMessage):
                            self._sock_event.clear()    
                            self._disconnect(sock)
                            sock = None
                            self._sock = sock
                        
                        elif isinstance(msg, MessageOut):
                            #print(f'Connection sending message {msg}')
                            _logger.info(f"CONNECTION SENDING MESSAGE: {msg}")
                            self._send(sock, msg.write())

                except TimeoutError:
                    #This is normal when there are no more mesages in the queue and wait time has ben statisfied. Just ignore it.
                    pass
                except Exception:
                    print_exc()
                    raise

        except SystemExit:
            self._notify_status(f'Shutdown signal received, exiting {self.name}')
        finally:
            try:
                self._disconnect(_sock)
                _sock = None
            except:
                pass

    def get_state(self):
        return self._state
      
    def _send_control_message(self, message:MessageOut):
        self._control_message = message
        self._deque_event.set()

    def _get_message(self, timeout=None):
        
        msg = None

        #Block until items are available
        if not self._deque_event.wait(timeout):
            raise TimeoutError()
        
        if self._control_message:
            msg = self._control_message
            self._control_message = None

        elif self._deque_message: 
            msg = self._deque_message.popleft()

        #If there are no more items, start blocking again
        if not self._deque_message:
            self._deque_event.clear()
        return msg

    def _set_config(self, config):
        self._config = config

    def _get_config(self):
        return self._config

    def _get_url(self):
        return self._config.get('url')

    def _get_delay(self):
        return self._config.get('delay')

    def _get_timeout(self):
        return self._config.get('timeout')

    def _set_state(self, state:ClientState):
        self._state = state
        msg = f'Connection state: {state}, url: {self._get_url()}'
        self._notify_status(msg)

    def _get_socket(self, timeout=None):
        if not self._sock_event.wait(timeout):
            raise socket.timeout()

        return self._sock

    def _notify_status(self, msg:str):
        #print(msg)
        _logger.info(msg)

    def _notify_error(self, msg:str):
        #print(msg)
        _logger.info(msg)

    def _connect(self, config:dict) -> socket:

        self._set_config(config)
        url = self._get_url()
        delay = self._get_delay()
        timeout = self._get_timeout()

        self._state_desired = ClientState.CONNECTED

        if self.get_state() != ClientState.DISCONNECTED:
            self._notify_status('Connection notice: already connected')
            return

        uparts = urlparse(url)

        sock = None

        self._set_state(ClientState.CONNECTING)
        end_time = time.time() + timeout

        while self._state_desired == ClientState.CONNECTED and (timeout == 0 or time.time() < end_time):
            try:
                sock = socket.socket()
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.settimeout(7)
                sock.connect((uparts.hostname, uparts.port))
                sock.settimeout(5)
                self._set_state(ClientState.CONNECTED)
                break
            except socket.timeout:
                if self._state_desired == ClientState.CONNECTED:
                    self._notify_status(f'Connection timeout: cannot connect to {url}, trying again in {delay} seconds')
                    time.sleep(delay)
            except ConnectionRefusedError:
                if self._state_desired == ClientState.CONNECTED:
                    self._notify_status(f'Connection refused: cannot connect to {url}, trying again in {delay} seconds')
                    time.sleep(delay)
            except Exception:
                print_exc()
                self._set_state(ClientState.DISCONNECTED)
                raise

        return sock

    def _disconnect(self, sock:socket):

        self._state_desired = ClientState.CONNECTED

        if self._state != ClientState.CONNECTED:
            self._notify_status('Connection notice: already disconnected')
            return

        self._set_state(ClientState.DISCONNECTING)
        #TODO[Giles]: Create lock around connect, shutdown, close, send and recv
        if sock:
            sock.shutdown(socket.SHUT_RDWR)
            sock.close()

        self._set_state(ClientState.DISCONNECTED)

    def _reconnect(self, sock: socket, config:dict) -> socket:
        self._disconnect(sock)
        if not config:
            config = self._get_config()
        time.sleep(3)
        return self._connect(config)

    def _send(self, sock:socket, buffer:bytes):
        if self.get_state() == ClientState.CONNECTED:
            #print(f'Socket sending message. len: {len(buffer)}, msg: {buffer}')
            _logger.debug(f'Socket sending message. len: {len(buffer)}, msg: {buffer}')
            sock.sendall(buffer)


class TcpipClientReaderWorker(AbortableThread):
    
    def __init__(self, connection_worker:TcpipClientConnectionWorker, message_reader:TcpipSocketReader, message_factory:MessageFactory, message_processor:MessageProcessor):
        super().__init__(name='tcpip client reader worker')
        self._connecton_worker = connection_worker
        self._msg_reader = message_reader
        self._msg_factory = message_factory
        self._msg_processor = message_processor

    def _notify_status(self, msg:str):
        print(msg)

    def run(self):
        try:
            while True:
                try:
                    #Can't wait forever in blocking call, need to enter loop to check for control messages, specifically SystemExit.
                    sock = self._connecton_worker._get_socket(5)
                    if sock:
                        buffer = self._msg_reader.read_socket(sock)
                        #print(f'Socket received message, len: {len(buffer)}, buffer: {buffer}')
                        _logger.debug(f'Socket received message, len: {len(buffer)}, buffer: {buffer}')

                        msg = self._msg_factory.create_message(buffer)
                        if msg:
                            #print(f'Message factory created: {msg}')
                            _logger.debug(f'Message factory created: {msg}')
                            self._msg_processor._queque_message(msg)

                except socket.timeout:
                    #Socket read timed out. This is normal, it just means that no messages have been sent so we can ignore it.
                    pass
                except ConnectionAbortedError:
                    #Connection is lost because the socket was closed, probably from the other side.
                    #Block the socket event and queue a reconnect message.
                    self._notify_status('Connection lost, attempting to reconnect')
                    self._connecton_worker._sock_event.clear()
                    self._connecton_worker.reconnect()
                except OSError as e:
                    if e.errno == errno.EBADF:
                        #Socket has been closed, probably from this side for some reason. Try to reconnect.
                        self._notify_status('Connection lost, attempting to reconnect')
                        self._connecton_worker._sock_event.clear()
                        self._connecton_worker.reconnect()
                    else:
                        raise
                except Exception as e:
                    print_exc()
                    raise

        except SystemExit:
            self._notify_status(f'Shutdown signal received, exiting {self.name}')
            
        finally:
            pass

class TcpipClientConnection(TcpipConnection):

    def __init__(self, url:str, config:dict, msg_processor:MessageProcessor, msg_reader:TcpipSocketReader, msg_factory:MessageFactory):
        cfg = {}
 
        if config:
            cfg.update(config)

        if cfg.get('delay', None) == None:
            cfg['delay'] = 7
        if cfg.get('timeout', None) == None:
            cfg['timeout'] = 300

        super().__init__(url, cfg)
        self._msg_reader = msg_reader
        self._msg_factory = msg_factory
        self._msg_processor = msg_processor
        self._connection_worker = TcpipClientConnectionWorker()
        self._connection_reader_worker = TcpipClientReaderWorker(self._connection_worker, self._msg_reader, self._msg_factory, self._msg_processor)

    def start(self):
        self._connection_worker.start()
        self._connection_reader_worker.start()

    def connect(self):
        """Set the desired connection state to ClientState.CONNECTED

        The connection will work in the background to establish a connection to the server.
        get_state() can be used to determine the current state of the connection.

        """
        cfg = dict(self._cfg)
        cfg['url'] = self._url
        self._connection_worker.connect(cfg)

    def disconnect(self):
        """Set the desired connection state to ClientState.DISCONNECTED

        The connection will work in the background to disconnect from the server.
        get_state() can be used to determine the current state of the connection.

        """
        self._connection_worker.disconnect()

    def get_state(self):
        """Return the current state of the connecion."""

        return self._connection_worker.get_state()

    def send(self, message: MessageOut):
        """Adds a message to the message queue. 
        
        The outgoing message processor will pull from the queue and send the message over the socket.
        If the socket is disconnected, the message will be thrown away.

        """
        self._connection_worker.send_message(message)

    def shutdown(self):
        self._connection_reader_worker.abort()
        self._connection_worker.abort()

    def wait(self):
        self._connection_reader_worker.join()
        self._connection_worker.join()
        self._connection_worker = None
        self._connection_reader_worker = None