import threading
import time
import logging
from socket import gaierror, gethostbyname
from requests import Response, Request, Session, Timeout, exceptions, request
from urllib.parse import urljoin, urlparse
from typing import Any
from enum import Enum
from pydhsfw.threads import AbortableThread
from pydhsfw.messages import BlockingQueue, MessageOut, MessageIn
from pydhsfw.transport import Transport, TransportState

_logger = logging.getLogger(__name__)

class RequestVerb(Enum):
    GET ='GET'
    POST = 'POST'

class Headers(Enum):
    DHS_REQUEST_TYPE_ID = 'DHS-Request-Type-Id'
    DHS_RESPONSE_TYPE_ID = 'DHS-Response-Type-Id'

class ResponseMessage(MessageIn):
    def __init__(self, response):
        super().__init__()
        self._response = response

    @staticmethod
    def parse_type_id(response:Response):
        return response.request.headers.get(Headers.DHS_RESPONSE_TYPE_ID.value)

    @classmethod
    def parse(cls, response:Response)->Any:
        
        msg = None
        
        type_id = ResponseMessage.parse_type_id(response)
        if type_id == cls.get_type_id():
            msg = cls(response)
        
        return msg

class JsonResponseMessage(ResponseMessage):
    def __init__(self, response):
        super().__init__(response)

    @property
    def json(self):
        return self._response.json()

class FileResponseMessage(ResponseMessage):
    def __init__(self, response):
        super().__init__(response)

    @property
    def file(self):
        return self._response.content

class GetRequestMessage(MessageOut):
    def __init__(self, path:str, params:dict=None):
        super().__init__()
        self._path = path
        self._params = params
    
    def write(self)->Request:
        request = Request(RequestVerb.GET.value, self._path, params=self._params)
        request.headers[Headers.DHS_REQUEST_TYPE_ID.value] = self.get_type_id()
        return request

    def __str__(self):
        return f'{super().__str__()} {self._path} {self._params}'

class PostRequestMessage(MessageOut):
    def __init__(self, path:str, json:dict=None, data:dict=None):
        super().__init__()
        self._path = path
        self._data = data
        self._json = json
    
    def write(self)->bytes:
        request = Request(RequestVerb.POST.value, self._path)
        request.headers[Headers.DHS_REQUEST_TYPE_ID.value] = self.get_type_id()
        if self._json:
            request.json = self._json
        elif self._data:
            request.data = self._data

        return request

    def __str__(self):
        return f'{super().__str__()} {self._path} {self._json} {self._data}'

class PostJsonRequestMessage(PostRequestMessage):
    def __init__(self, path:str, json:dict):
        super().__init__(path, json=json)

class PostFormRequestMessage(PostRequestMessage):
    def __init__(self, path:str, data:dict):
        super().__init__(path, data=data)

class MessageResponseReader():
    def __init__(self):
        pass

    def read_response(self, response:Response)->Response:
        ''' Read the http response and convert it into an object that the message factory can read and convert to a message.

        '''
        return response

class MessageRequestWriter():
    def __init__(self):
        pass

    def write_request(self, request:Request)->Request:
        ''' Create an http request out of a message bytes.

        '''

        # Read the custom header for the message request type id and add a custom header with the response
        # message type id which is just replacing the _request at the end with _response
        req_type_id = request.headers.get(Headers.DHS_REQUEST_TYPE_ID.value) 
        if req_type_id:
            res_type_id = req_type_id
            if res_type_id.endswith('_request'): 
                 res_type_id = res_type_id[:-(len('_request'))]
            res_type_id += '_response'
            request.headers[Headers.DHS_RESPONSE_TYPE_ID.value] = res_type_id

        return request


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

            url = self._get_heartbeat_url()
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

                    elif self._state == TransportState.CONNECTED:
                        if self._next_heatbeat and time.time() > self._next_heatbeat:
                            _logger.info(f'Sending heartbeat to {url}')
                            state = self._heartbeat(url, self._get_blocking_timeout())
                            if state != TransportState.CONNECTED:
                                _logger.warning(f'Heartbeat failed: cannot connect to {url}')
                                self._set_state(state)

                except gaierror as e:
                    _logger.error(f'Connection error, could not resolve hostname {urlparse(url).hostname}')
                    self._set_state(TransportState.DISCONNECTED)
                except exceptions.ConnectionError as e:
                    _logger.error(f'Connection error, could not connect to {url}')
                    self._set_state(TransportState.DISCONNECTED)
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
        if state in (TransportState.CONNECTED, TransportState.DISCONNECTED):
            #TODO[Giles]: Add ConnectionConnectedMessage or ConnectionDisconnectedMessage to the queue.
            pass

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
                            _logger.info(f'Connecting to {url}')
                            state = self._heartbeat(url, wait_timeout)
                            if state == TransportState.CONNECTED:
                                self._set_state(TransportState.CONNECTED)
                                break
                        else:
                            time.sleep(wait_timeout)

                    except Timeout:
                        if self._desired_state == TransportState.CONNECTED:
                            _logger.info(f'Connection timeout: cannot connect to {url}, trying again in {connect_retry_delay} seconds')
                            end_delay_time = time.time() + connect_retry_delay
                    except gaierror as e:
                        _logger.error(f'Connection error, could not resolve hostname {urlparse(url).hostname}')
                        self._set_state(TransportState.DISCONNECTED)
                    except exceptions.ConnectionError as e:
                        _logger.error(f'Connection error, could not connect to {url}')
                        self._set_state(TransportState.DISCONNECTED)
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
        
        #Use DNS lookup to resolve hostname since the request call below takes a long time.
        gethostbyname(urlparse(url).hostname)
        hearbeat_delay = self._config.get('heartbeat_delay', 30)
        self._next_heatbeat = time.time() + hearbeat_delay
        response = request('GET', url, timeout=timeout)
        if response.ok:
            state = TransportState.CONNECTED
        
        return state

class ResponseQueue(BlockingQueue[Response]):
    def __init__(self):
        super().__init__()

class HttpClientTransport(Transport):
    ''' Http client transport '''

    def __init__(self, url:str, message_reader:MessageResponseReader, message_writer:MessageRequestWriter, config:dict={}):
        super().__init__(url, config)
        self._message_reader = message_reader
        self._message_writer = message_writer
        self._connection_worker = HttpClientTransportConnectionWorker(url, config)
        self._response_queue = ResponseQueue()

    def _send(self, request:Request)->Response:
        response = None
        if self._connection_worker._state == TransportState.CONNECTED:
            with Session() as s:
                p = s.prepare_request(request)
                response = s.send(p)
        return response

    def send(self, msg:Request):
        try:
            if self._connection_worker._state == TransportState.CONNECTED:
                request = self._message_writer.write_request(msg)
                # Add the path to the base url.
                request.url = urljoin(self._url, request.url)
                response = self._send(request)
                if response.ok:
                    self._response_queue.queque(response)
                else:
                    _logger.warning(f'Bad response {response.status_code}')

            else:
                _logger.warning(f'Send failed, not connected {msg}')
            
        except Exception:
            #Connection is lost because the socket was closed, probably from the other side.
            #Block the socket event and queue a reconnect message.
            _logger.exception(None)
            raise
            #self.reconnect()

    def receive(self)->Response:
        try:
            response = self._response_queue.fetch(self._connection_worker._get_blocking_timeout())
            if response: 
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

