import base64
import logging
from typing import Any
from dotty_dict import dotty
from pydhsfw.messages import IncomingMessageQueue, OutgoingMessageQueue, MessageFactory, register_message
from pydhsfw.connection import ConnectionBase, register_connection
from pydhsfw.http import PostJsonRequestMessage, JsonResponseMessage, HttpClientTransport, MessageResponseReader, MessageRequestWriter, ResponseMessage

_logger = logging.getLogger(__name__)

@register_message('automl_predict_request')
class AutoMLPredictRequest(PostJsonRequestMessage):
    """Formats a json data package and sends to the GCP AutoML server for prediction."""
    
    def __init__(self, key:str, image:bytes):

        encoded_image = base64.b64encode(image).decode('utf-8')

        instances = {
            'instances': [
                {'image_bytes': {'b64': str(encoded_image)},
                'key': key}
            ],
            'params': [
                {'max_bounding_box_count': '10'}
            ]
        }

        super().__init__('/v1/models/default:predict', json=instances)

@register_message('automl_predict_response', 'automl')
class AutoMLPredictResponse(JsonResponseMessage):
    """Parses the response json file and stores various values the top hit in class properties."""

    def __init__(self, response):
        super().__init__(response)

    def get_score(self,n:int)->float:
        """Returns the AutoML inference score for the Nth object in a sorted results list"""
        #_logger.info(f'AutoMLPredictResponse function called with {n}')
        return dotty(self.json)[f'predictions.0.detection_scores.{n}']

    @property
    def image_key(self):
        return dotty(self.json)['predictions.0.key']

    @property
    def top_score(self):
       # might want to do some sort of filtering here?
       # only accept if score if better than 90% or something?
        return dotty(self.json)['predictions.0.detection_scores.0']

    @property
    def top_bb(self):
        """
        Object bounding box returned from AutoML
        minY, minX, maxY, maxX

        origin in upper left corner of image.

        (0,0)----------------------> (1,0) X-axis
        |
        |
        |
        |
        v
        (0,1) Y-axis

        """
        return dotty(self.json)['predictions.0.detection_boxes.0']

    @property
    def bb_minY(self):
        return dotty(self.json)['predictions.0.detection_boxes.0.0']

    @property
    def bb_minX(self):
        return dotty(self.json)['predictions.0.detection_boxes.0.1']

    @property
    def bb_maxY(self):
        return dotty(self.json)['predictions.0.detection_boxes.0.2']

    @property
    def bb_maxX(self):
        return dotty(self.json)['predictions.0.detection_boxes.0.3']

    @property
    def top_classification(self):
        return dotty(self.json)['predictions.0.detection_classes_as_text.0']

class AutoMLMessageFactory(MessageFactory):
    def __init__(self):
        super().__init__('automl')

    def _parse_type_id(self, response:Any):
        return ResponseMessage.parse_type_id(response)

class AutoMLClientTransport(HttpClientTransport):
    """Overrides HttpClientTransport and handles message reading and writing"""

    def __init__(self, connection_name:str, url:str, config:dict={}):
        super().__init__(connection_name, url, MessageResponseReader(), MessageRequestWriter(), config)

@register_connection('automl')
class AutoMLClientConnection(ConnectionBase):
    """Overrides ConnectionBase and creates a GCP AutoML connection"""

    def __init__(self, connection_name:str, url:str, incoming_message_queue:IncomingMessageQueue, outgoing_message_queue:OutgoingMessageQueue, config:dict={}):
        super().__init__(connection_name, url, AutoMLClientTransport(connection_name, url, config), incoming_message_queue, outgoing_message_queue, AutoMLMessageFactory(), config)
