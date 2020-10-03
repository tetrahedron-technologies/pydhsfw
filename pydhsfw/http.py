import threading
import time
import logging
import requests
from urllib.parse import urlparse, urljoin
from typing import Any
from pydhsfw.threads import AbortableThread
from pydhsfw.messages import BlockingMessageQueue
from pydhsfw.transport import Transport, TransportState

_logger = logging.getLogger(__name__)

class MessageResponseReader():
    def __init__(self):
        pass

    def read_response(self, response:requests.Response)->Any:
        ''' Read the http response and convert it into an object that the message factory can read and convert to a message.

        '''
        pass

class MessageRequestWriter():
    def __init__(self):
        pass

    def write_request(self, request:Any)->requests.Request:
        ''' Create an http request out of a message object.

        '''
        pass



class HttpClientTransportConnectionWorker(AbortableThread):

    def __init__(self, url:str, config:dict={}):
        super().__init__(name='http client transport connection worker', config=config)
        self._url = url
        self._hearbeat_path = config.get('heartbeat_path')
        self._config = config
        self._state = TransportState.DISCONNECTED
        self._desired_state = TransportState.DISCONNECTED
        self._state_change_event = threading.Event()
        self._next_heatbeat = None

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

        # 1. Attempt to connect by hitting a heartbeat url.
        # 2. Keep hitting the heartbeat url occasionally to make sure the connection is still there.
        try:

            while True:
                try:
                    if self._state_change_event.wait(self._get_blocking_timeout()):
                        # Can't wait forever in blocking call, need to enter loop to check for control messages, specifically SystemExit.

                        if self._desired_state == TransportState.CONNECTED:
                            self._connect()
                        elif self._desired_state == TransportState.DISCONNECTED:
                            self._disconnect()
                        elif self._desired_state == TransportState.RECONNECTED:
                            self._reconnect()

                        self._state_change_event.clear()

                    else:
                        if self._next_heatbeat and time() > self._next_heatbeat:
                            state = self._heartbeat(self._get_heartbeat_url(), self._get_blocking_timeout)
                            if state != TransportState.CONNECTED:
                                _logger.warning(f'Heartbeat failed: cannot connect to {self._get_heartbeat_url()}')
                                self._set_state(state)


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

    def _get_heartbeat_url(self):
        return urljoin(self._url, self._hearbeat_path)

    def _set_state(self, state:TransportState):
        self._state = state
        _logger.info(f'Connection state: {state}, url: {self._get_url()}')

    def _connect(self):

        if self._desired_state == TransportState.CONNECTED:

            if self.state == TransportState.DISCONNECTED:

                wait_timeout = self._get_blocking_timeout()
                connect_timeout = self._config.get('connect_timeout', None)
                connect_retry_delay = self._config.get('connect_retry_delay', 10)
                url = self._get_heartbeat_url()

                self._set_state(TransportState.CONNECTING)

                end_time = time.time() + float(connect_timeout or 0.0)
                end_delay_time = time.time()

                while self._desired_state == TransportState.CONNECTED and (connect_timeout == None or time.time() < end_time):
                    try:
                        if time.time() >= end_delay_time:
                            state = self._hearbeat(url, wait_timeout)
                            if state == TransportState.CONNECTED:
                                self._set_state(TransportState.CONNECTED)
                                break
                        else:
                            time.sleep(wait_timeout)

                    except requests.Timeout:
                        if self._desired_state == TransportState.CONNECTED:
                            _logger.info(f'Connection timeout: cannot connect to {url}, trying again in {connect_retry_delay} seconds')
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

    def _heartbeat(self, url, timeout)->TransportState:
        state = TransportState.DISCONNECTED
        hearbeat_delay = self._config.get('heartbeat_delay', 30)
        self._next_heatbeat = time() + hearbeat_delay
        _logger.info(f'Sending heartbeat to {url}')
        response = requests.request('GET', url, timeout=timeout)
        if response.ok:
            state = TransportState.CONNECTED
        
        return state

class HttpClientTransport(Transport):
    ''' Http client transport '''

    def __init__(self, url:str, message_reader:MessageResponseReader, message_writer:MessageRequestWriter, config:dict={}):
        super().__init__(url, config)
        self._message_reader = message_reader
        self._message_writer = message_writer
        self._connection_worker = HttpClientTransportConnectionWorker(url, config)
        self._msg_queue = BlockingMessageQueue

    def send(self, msg:Any):
        try:
            if self._connection_worker._state == TransportState.CONNECTED:
                request = self._message_writer.write_request(msg)
                s = requests.Session()
                p = s.prepare_request(request)
                response = s.send(p)
                self._msg_queue._queque_message(response)
            
        except Exception:
            #Connection is lost because the socket was closed, probably from the other side.
            #Block the socket event and queue a reconnect message.
            _logger.exception(None)
            raise
            #self.reconnect()

    def receive(self):
        try:
            response = self._msg_queue._get_message(self._connection_worker._get_blocking_timeout)
            return self._message_reader.read_response(response)
        except TimeoutError:
            #Read timed out. This is normal, it just means that no messages have been sent so we can ignore it.
            pass
        except Exception:
            #Connection is lost because the socket was closed, probably from the other side.
            #Block the socket event and queue a reconnect message.
            _logger.excpetion(None)
            raise
            #self.reconnect()

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

