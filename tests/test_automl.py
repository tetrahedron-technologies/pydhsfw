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
from pydhsfw.messages import IncomingMessageQueue, MessageFactory, OutgoingMessageQueue, register_message
from pydhsfw.connection import ConnectionBase, register_connection
from pydhsfw.processors import register_message_handler
from pydhsfw.http import PostJsonRequestMessage, JsonResponseMessage, HttpClientTransport, MessageResponseReader, MessageRequestWriter, ResponseMessage
from pydhsfw.dhs import Dhs, DhsContext, DhsInit, DhsStart

_logger = logging.getLogger(__name__)

@register_message('automl_predict_request')
class AutoMLPredictRequest(PostJsonRequestMessage):
    def __init__(self, key:str, image:bytes):

        encoded_image = base64.b64encode(image).decode('utf-8')

        instances = {
            'instances': [
                {'image_bytes': {'b64': str(encoded_image)},
                'key': key}
            ]
        }

        super().__init__('/v1/models/default:predict', json=instances)

@register_message('automl_predict_response', 'automl')
class AutoMLPredictResponse(JsonResponseMessage):
    def __init__(self, response):
        super().__init__(response)

    @property
    def top_result(self):
        return dotty(self.json)['predictions.0.detection_scores.0']
    
    @property
    def top_bb(self):
        return dotty(self.json)['predictions.0.detection_boxes.0']

    @property
    def top_classification(self):
        return dotty(self.json)['predictions.0.detection_classes_as_text.0']

class AutoMLMessageFactory(MessageFactory):
    def __init__(self):
        super().__init__('automl')

    def _parse_type_id(self, response:Any):
        return ResponseMessage.parse_type_id(response)

class AutoMLClientTransport(HttpClientTransport):
    def __init__(self, url:str, config:dict={}):
        super().__init__(url, MessageResponseReader(), MessageRequestWriter(), config)

@register_connection('automl')
class AutoMLClientConnection(ConnectionBase):
    def __init__(self, url:str, incoming_message_queue:IncomingMessageQueue, outgoing_message_queue:OutgoingMessageQueue, config:dict={}):
        super().__init__(url, AutoMLClientTransport(url, config), incoming_message_queue, outgoing_message_queue, AutoMLMessageFactory(), config)

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

    context.create_connection('automl_conn', 'automl', url, {'heartbeat_path': '/v1/models/default'})
    context.get_connection('automl_conn').connect()
    time.sleep(3)
    with io.open(filename, 'rb') as image_file:
        binary_image = image_file.read()
    context.get_connection('automl_conn').send(AutoMLPredictRequest(image_key,binary_image))

@register_message_handler('automl_predict_response')
def automl_query_response(message:AutoMLPredictResponse, context:DhsContext):
    _logger.info(message.top_result)
    # do stuff n math n things
    # figurte out which op
    # send operation updates to dcss

dhs = Dhs()
dhs.start()
sigs = {}
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)