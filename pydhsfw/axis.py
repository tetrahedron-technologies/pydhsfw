#axis.py
import logging
from typing import Any
from pydhsfw.messages import IncomingMessageQueue, OutgoingMessageQueue, MessageFactory
from pydhsfw.connection import ConnectionBase, register_connection
from pydhsfw.http import HttpClientTransport, MessageResponseReader, MessageRequestWriter, ResponseMessage, GetRequestMessage, FileResponseMessage

_logger = logging.getLogger(__name__)


@register_message('axis_image_request')
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

@register_message('axis_image_response')
class FileResponseMessage(ResponseMessage):
    def __init__(self, response):
        super().__init__(response)

    @property
    def file(self):
        return self._response.content

class AxisMessageFactory(MessageFactory):
    def __init__(self):
        super().__init__('axis')

    def _parse_type_id(self, response:Any):
        return ResponseMessage.parse_type_id(response)
    
class AxisClientTransport(HttpClientTransport):
    """Overrides HttpClientTransport and handles message reading and writing"""

    def __init__(self, connection_name:str, url:str, config:dict={}):
        super().__init__(connection_name, url, MessageResponseReader(), MessageRequestWriter(), config)
        
@register_connection('axis')
class AxisClientConnection(ConnectionBase):
    """Overrides ConnectionBase and creates an HTTP Axis connection"""

    def __init__(self, connection_name:str, url:str, incoming_message_queue:IncomingMessageQueue, outgoing_message_queue:OutgoingMessageQueue, config:dict={}):
        super().__init__(connection_name, url, AxisClientTransport(connection_name, url, config), incoming_message_queue, outgoing_message_queue, AxisMessageFactory(), config)

