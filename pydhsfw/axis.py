#axis.py
import logging
from typing import Any
from pydhsfw.messages import IncomingMessageQueue, OutgoingMessageQueue, MessageFactory
from pydhsfw.connection import ConnectionBase, register_connection
from pydhsfw.http import HttpClientTransport, MessageResponseReader, MessageRequestWriter, ResponseMessage  

_logger = logging.getLogger(__name__)


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

