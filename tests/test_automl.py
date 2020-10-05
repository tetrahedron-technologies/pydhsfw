import base64
import io
import json
from context import pydhsfw
import logging
import sys
import time
import signal
from typing import Any
from dotty_dict import dotty
from pydhsfw.messages import MessageFactory, MessageQueue, register_message
from pydhsfw.connection import ConnectionBase, register_connection
from pydhsfw.processors import register_message_handler
from pydhsfw.http import *
from pydhsfw.dhs import Dhs, DhsContext, DhsInit, DhsStart

_logger = logging.getLogger(__name__)

@register_message('automl_query_request')
class AutoMLQueryRequest(PostRequestMessage):
    def __init__(self, query:str):
        super().__init__('/v1/models/default:predict', params={'q':query, 'format':'json'})


@register_message('automl_query_response', 'automl')
class AutoMLQueryResponse(JsonResponseMessage):
    def __init__(self, response):
        super().__init__(response)

    @property
    def top_result(self):
        return dotty(self.json)['RelatedTopics.0.Text']

class AutoMLMessageFactory(MessageFactory):
    def __init__(self):
        super().__init__('automl')

    def _parse_type_id(self, response:Any):
        return ResponseMessage.parse_type_id(response)

class AutoMLClientTransport(HttpClientTransport):
    def __init__(self, url:str, config:dict={}):
        super().__init__(url, MessageResponseReader(), MessageRequestWriter(), config)

@register_connection('automl')
class DuckClientConnection(ConnectionBase):
    def __init__(self, url:str, incoming_message_queue:MessageQueue, outgoing_message_queue:MessageQueue, config:dict={}):
        super().__init__(url, AutoMLClientTransport(url, config), incoming_message_queue, outgoing_message_queue, AutoMLMessageFactory(), config)

@register_message_handler('dhs_init')
def dhs_init(message:DhsInit, context:DhsContext):
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(funcName)s():%(lineno)d - %(message)s"
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S")

@register_message_handler('dhs_start')
def dhs_start(message:DhsStart, context:DhsContext):
    port_number = 5000
    image_key = '1a2s3d4f5g'
    with io.open('loop_nylon.jpg', 'rb') as image_file:
       encoded_image = base64.b64encode(image_file.read()).decode('utf-8')
    instances = {
        'instances': [
            {'image_bytes': {'b64': str(encoded_image)},
             'key': image_key}
        ]
    }
    url = 'http://localhost:{}/v1/models/default:predict'.format(port_number)

    #response = requests.post(url, data=json.dumps(instances))
    #print(response.json())

    context.create_connection('automl_conn', 'automl', url)
    context.get_connection('automl_conn').connect()

    time.sleep(3)

    context.get_connection('automl_conn').send(AutoMLQueryRequest('what is the meaning of life'))

dhs = Dhs()
dhs.start()
sigs = {}
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)