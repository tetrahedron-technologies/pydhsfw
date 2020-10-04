from context import pydhsfw
import logging
import sys
import time
from typing import Any
import signal
from pydhsfw.messages import MessageFactory, MessageQueue, register_message
from pydhsfw.connection import ConnectionBase, register_connection
from pydhsfw.processors import register_message_handler
from pydhsfw.http import GetRequestMessage, HttpClientTransport, JsonResponseMessage, MessageResponseReader, MessageRequestWriter, ResponseMessage
from pydhsfw.dhs import Dhs, DhsContext, DhsInit, DhsStart

_logger = logging.getLogger(__name__)

@register_message('duck_query_request')
class DuckQueryRequest(GetRequestMessage):
    def __init__(self, query:str):
        super().__init__('/', params={'q':query, 'format':'json'})


@register_message('duck_query_response', 'duck')
class DuckQueryResponse(JsonResponseMessage):
    def __init__(self, response):
        super().__init__(response)

    @property
    def top_result(self):
        return self.json['RelatedTopics'][0]['Text']

class DuckMessageFactory(MessageFactory):
    def __init__(self):
        super().__init__('duck')

    def _parse_type_id(self, response:Any):
        return ResponseMessage.parse_type_id(response)

class DuckClientTransport(HttpClientTransport):
    def __init__(self, url:str, config:dict={}):
        super().__init__(url, MessageResponseReader(), MessageRequestWriter(), config)

@register_connection('duck')
class DuckClientConnection(ConnectionBase):
    def __init__(self, url:str, incoming_message_queue:MessageQueue, outgoing_message_queue:MessageQueue, config:dict={}):
        super().__init__(url, DuckClientTransport(url, config), incoming_message_queue, outgoing_message_queue, DuckMessageFactory(), config)


@register_message_handler('dhs_init')
def dhs_init(message:DhsInit, context:DhsContext):
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(funcName)s():%(lineno)d - %(message)s"
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S")

@register_message_handler('dhs_start')
def dhs_start(message:DhsStart, context:DhsContext):
    url = 'https://api.duckduckgo.com'

    context.create_connection('duck_conn', 'duck', url)
    context.get_connection('duck_conn').connect()

    time.sleep(2)

    context.get_connection('duck_conn').send(DuckQueryRequest('what time is it'))
    
@register_message_handler('duck_query_response')
def duck_query_response(message:DuckQueryResponse, context:DhsContext):
    _logger.info(message.top_result)

dhs = Dhs()
dhs.start()
sigs = {}
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)