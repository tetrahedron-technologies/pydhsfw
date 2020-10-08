from context import pydhsfw
import threading
import logging
import sys
import time
import signal
import http.server
from http import HTTPStatus
from functools import partial
from urllib.parse import urlparse
from pydhsfw.threads import AbortableThread
from pydhsfw.transport import Transport, TransportState
from typing import Any
from requests import Request
from pydhsfw.messages import IncomingMessageQueue, OutgoingMessageQueue, MessageFactory, register_message
from pydhsfw.connection import ConnectionBase, register_connection
from pydhsfw.processors import register_message_handler
from pydhsfw.http import ContentType, FileServerRequestMessage, Headers, RequestQueue, RequestVerb, ServerMessageRequestReader, ServerRequestMessage
from pydhsfw.dhs import Dhs, DhsContext, DhsInit, DhsStart

_logger = logging.getLogger(__name__)

class AxisServerMessageRequestReader(ServerMessageRequestReader):
    def __init__(self):
        pass

    def read_request(self, request:Request)->Request:
        if request.method == RequestVerb.GET.value:
            request.headers[Headers.DHS_REQUEST_TYPE_ID.value] = 'axissvr_get_request'
        elif request.method == RequestVerb.POST.value:
            content_type = request.headers.get(Headers.CONTENT_TYPE.value)
            if content_type == ContentType.JPEG.value:
                request.headers[Headers.DHS_REQUEST_TYPE_ID.value] = 'axissvr_image_post_request'

        return request

class AxisServerRequestHandler(http.server.BaseHTTPRequestHandler):
    def __init__(self, request_queue:RequestQueue, *args, **kwargs):
        self._request_queue = request_queue
        super().__init__(*args, **kwargs)

    protocol_version = 'HTTP/1.1'
    timeout = 5

    def do_GET(self):
        headers = dict(self.headers)
        request = Request(method='GET', url=self.path, headers=headers)
        self.send_response(HTTPStatus.OK)
        self.end_headers()
        self._request_queue.queque(request)

    def do_POST(self):

        data_len_hdr = self.headers.get('Content-Length')
        data_len = int(data_len_hdr)

        remainingbytes = data_len

        data = bytes()
        while remainingbytes > 0:
            chunk = self.rfile.read(remainingbytes)
            if chunk == b'':
                break
            data += chunk
            remainingbytes -= len(chunk)
    
        self.send_response(HTTPStatus.OK)
        self.send_header('Connection', 'close')
        self.end_headers()

        request = Request(method='POST', url=self.path, headers=dict(self.headers), data=data)
        self._request_queue.queque(request)

    def log_message(self, format: str, *args: Any) -> None:
        _logger.info("%s - %s" % (self.address_string(), format%args))
    
    def log_error(self, format: str, *args: Any) -> None:
        _logger.error("%s - %s" % (self.address_string(), format%args))

    def handle_expect_100(self):
        self.log_request(HTTPStatus.CONTINUE)
        self.send_response_only(HTTPStatus.CONTINUE)
        self.end_headers()
        return True    

class HttpAbortableServer(http.server.HTTPServer):
    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True):
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)

    def shutdown_trigger(self):
        self.__shutdown_request = True

class AxisServerTransportConnectionWorker(AbortableThread):

    def __init__(self, connection_name:str, url:str, request_queue:RequestQueue, config:dict={}):
        super().__init__(name=f'{connection_name} axis server transport connection worker', config=config)
        self._connection_name = connection_name
        self._url = url
        self._config = config
        self._state = TransportState.DISCONNECTED
        self._desired_state = TransportState.DISCONNECTED
        self._state_change_event = threading.Event()
        self._request_queue = request_queue
        request_hander = partial(AxisServerRequestHandler, self._request_queue)
        self._http_server = HttpAbortableServer(('', urlparse(url).port), request_hander)

    def connect(self):
        self._set_desired_state(TransportState.CONNECTED)

    def reconnect(self):
        self._set_desired_state(TransportState.RECONNECTED)

    def disconnect(self):
        self._set_desired_state(TransportState.DISCONNECTED)
        self._disconnect()

    def _set_desired_state(self, state:TransportState):
        if self._desired_state != state:
            self._desired_state = state
            self._state_change_event.set()

    def run(self):

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


    def _connect(self):
        try:
            self._http_server.serve_forever(self._get_blocking_timeout())
        except KeyboardInterrupt:
            _logger.exception(None)
            raise
        except Exception:
            _logger.exception(None)
            raise

    def _disconnect(self):
        self._http_server.shutdown_trigger()

    def _reconnect(self):
        self._http_server.shutdown()
        time.sleep(self._get_blocking_timeout())
        self._connect()


class AxisImageServerTransport(Transport):
    ''' Http server transport '''

    def __init__(self, connection_name:str, url:str, message_reader:AxisServerMessageRequestReader, config:dict={}):
        super().__init__(connection_name, url, config)
        self._poll_timeout = config.get(AbortableThread.THREAD_BLOCKING_TIMEOUT, AbortableThread.THREAD_BLOCKING_TIMEOUT_DEFAULT)
        self._message_reader = message_reader
        self._request_queue = RequestQueue()
        self._connection_worker = AxisServerTransportConnectionWorker(connection_name, url, self._request_queue, config)

    def send(self, msg:Any):
        raise NotImplemented

    def receive(self)->Request:
        try:
            request = self._request_queue.fetch(self._poll_timeout)
            if request: 
                return self._message_reader.read_request(request)
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
        self.disconnect()
        self._connection_worker.abort()

    def wait(self):
        self._connection_worker.join()

@register_message('axissvr_get_request', 'axissvr')
class AxisServerGetRequestMessage(ServerRequestMessage):
    def __init__(self, request):
        super().__init__(request)

@register_message('axissvr_image_post_request', 'axissvr')
class AxisServerImagePostRequestMessage(FileServerRequestMessage):
    def __init__(self, request):
        super().__init__(request)

class AxisImageServerMessageFactory(MessageFactory):
    def __init__(self):
        super().__init__('axissvr')

    def _parse_type_id(self, request:Any):
        return ServerRequestMessage.parse_type_id(request)

class AxisImageServerTransport(AxisImageServerTransport):
    def __init__(self, connection_name:str, url:str, config:dict={}):
        super().__init__(connection_name, url, AxisServerMessageRequestReader(), config)

@register_connection('axissvr')
class AxisImageServerConnection(ConnectionBase):
    def __init__(self, connection_name:str, url:str, incoming_message_queue:IncomingMessageQueue, outgoing_message_queue:OutgoingMessageQueue, config:dict={}):
        super().__init__(connection_name, url, AxisImageServerTransport(connection_name, url, config), incoming_message_queue, outgoing_message_queue, AxisImageServerMessageFactory(), config)



#######################################################


@register_message_handler('dhs_init')
def dhs_init(message:DhsInit, context:DhsContext):
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(funcName)s():%(lineno)d - %(message)s"
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S")

@register_message_handler('dhs_start')
def dhs_start(message:DhsStart, context:DhsContext):
    url = 'https://:7171'

    context.create_connection('axis_svr_conn', 'axissvr', url)
    context.get_connection('axis_svr_conn').connect()

    time.sleep(3)

@register_message_handler('axissvr_get_request')
def axis_image_request(message:AxisServerGetRequestMessage, context:DhsContext):
    _logger.info(message)
    
@register_message_handler('axissvr_image_post_request')
def axis_image_request(message:AxisServerImagePostRequestMessage, context:DhsContext):
    _logger.debug(message.file)

dhs = Dhs()
dhs.start()
sigs = {}
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)