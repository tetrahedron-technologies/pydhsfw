import base64
import io
import json
import logging
import sys
import time
import signal
from typing import Any
from dotty_dict import dotty
from pydhsfw.messages import MessageFactory, MessageQueue, register_message
from pydhsfw.connection import ConnectionBase, register_connection
from pydhsfw.processors import register_message_handler
from pydhsfw.http import PostJsonRequestMessage, JsonResponseMessage, HttpClientTransport, MessageResponseReader, MessageRequestWriter, ResponseMessage
from pydhsfw.dhs import Dhs, DhsContext, DhsInit, DhsStart

_logger = logging.getLogger(__name__)

@register_message('automl_predict_request')
class AutoMLPredictRequest(PostJsonRequestMessage):
    """AutoMLPredictRequest

        formats the json data package and sends to the GCP AutoML server for prediction
    """
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
    """AutoMLPredictResponse

        parses the response json file and stores vrious values the top hit in class properties
    """
    def __init__(self, response):
        super().__init__(response)

    @property
    def top_result(self):
       # might want to do some sort of filtering here?
       # only accept if score if better than 90% or something?
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
    def __init__(self, url:str, incoming_message_queue:MessageQueue, outgoing_message_queue:MessageQueue, config:dict={}):
        super().__init__(url, AutoMLClientTransport(url, config), incoming_message_queue, outgoing_message_queue, AutoMLMessageFactory(), config)
