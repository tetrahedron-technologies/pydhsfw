import io
from context import pydhsfw
import logging
import sys
import time
import signal
from typing import Any
from pydhsfw.messages import IncomingMessageQueue, OutgoingMessageQueue, MessageFactory, register_message
from pydhsfw.connection import ConnectionBase, register_connection
from pydhsfw.processors import register_message_handler
from pydhsfw.http import HttpClientTransport, MessageResponseReader, MessageRequestWriter, ResponseMessage
from pydhsfw.dhs import Dhs, DhsContext, DhsInit, DhsStart

_logger = logging.getLogger(__name__)


class AxisMessageFactory(MessageFactory):
    def __init__(self):
        super().__init__('axis')

    def _parse_type_id(self, response:Any):
        return ResponseMessage.parse_type_id(response)

class AxisClientTransport(HttpClientTransport):
    def __init__(self, connection_name:str, url:str, config:dict={}):
        super().__init__(connection_name, url, MessageResponseReader(), MessageRequestWriter(), config)

@register_connection('axis')
class AxisClientConnection(ConnectionBase):
    def __init__(self, connection_name:str, url:str, incoming_message_queue:IncomingMessageQueue, outgoing_message_queue:OutgoingMessageQueue, config:dict={}):
        super().__init__(connection_name, url, AxisClientTransport(connection_name, url, config), incoming_message_queue, outgoing_message_queue, AxisMessageFactory(), config)

@register_message_handler('dhs_init')
def dhs_init(message:DhsInit, context:DhsContext):
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(funcName)s():%(lineno)d - %(message)s"
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S")

@register_message_handler('dhs_start')
def dhs_start(message:DhsStart, context:DhsContext):

    port_number = 5000
    image_key = '1a2s3d4f5g'
    filename = 'loop_nylon.jpg'
    url = 'http://localhost:{}'.format(port_number)

    context.create_connection('axis_conn', 'axis', url, {'path': 'axis-cgi/jpg/image.cgi'})
    context.get_connection('axis_conn').connect()
    time.sleep(3)

    _logger.info(message.top_result)

dhs = Dhs()
dhs.start()
sigs = {}
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)
