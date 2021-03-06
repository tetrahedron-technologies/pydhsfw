# -*- coding: utf-8 -*-
import base64
import logging
from typing import Any
from dotty_dict import dotty
from pydhsfw.messages import (
    IncomingMessageQueue,
    OutgoingMessageQueue,
    MessageFactory,
    register_message,
)
from pydhsfw.connection import ConnectionBase, register_connection
from pydhsfw.http import (
    PostJsonRequestMessage,
    JsonResponseMessage,
    HttpClientTransport,
    MessageResponseReader,
    MessageRequestWriter,
    ResponseMessage,
)

_logger = logging.getLogger(__name__)


@register_message('automl_predict_request')
class AutoMLPredictRequest(PostJsonRequestMessage):
    """Formats a json data package and sends to the GCP AutoML server for prediction."""

    def __init__(self, key: str, image: bytes):

        encoded_image = base64.b64encode(image).decode('utf-8')

        instances = {
            'instances': [{'image_bytes': {'b64': str(encoded_image)}, 'key': key}],
            'params': [{'max_bounding_box_count': '10'}],
        }

        super().__init__('/v1/models/default:predict', json=instances)


@register_message('automl_predict_response', 'automl')
class AutoMLPredictResponse(JsonResponseMessage):
    """Parses the response json file and stores various values the top hit in class properties."""

    def __init__(self, response):
        super().__init__(response)
        self._pin_num = None
        self._loop_num = None
        # could we assign values to these two variables during __init__ ?
        # if these are "None" when any properties are accessed then we will get an error.

    def get_score(self, n: int) -> float:
        """Returns the AutoML inference score for the Nth object in a sorted results list"""
        # _logger.info(f'AutoMLPredictResponse function called with {n}')
        return dotty(self.json)[f'predictions.0.detection_scores.{n}']

    def get_detection_class_as_text(self, n: int) -> str:
        """Returns the AutoML classification (as text) for the Nth object in a sorted results list"""
        return dotty(self.json)[f'predictions.0.detection_classes_as_text.{n}']

    def get_detection_class_as_int(self, n: int) -> str:
        """Returns the AutoML classification (as int) for the Nth object in a sorted results list"""
        return dotty(self.json)[f'predictions.0.detection_classes.{n}']

    @property
    def pin_num(self):
        return self._pin_num

    @pin_num.setter
    def pin_num(self, n: int):
        self._pin_num = n

    @property
    def loop_num(self):
        return self._loop_num

    @loop_num.setter
    def loop_num(self, n: int):
        self._loop_num = n

    @property
    def image_key(self):
        return dotty(self.json)['predictions.0.key']

    @property
    def loop_top_score(self):
        # might want to do some sort of filtering here?
        # only accept if score if better than 90% or something?
        return dotty(self.json)[f'predictions.0.detection_scores.{self._loop_num}']

    @property
    def loop_top_bb(self):
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
        return dotty(self.json)[f'predictions.0.detection_boxes.{self._loop_num}']

    @property
    def loop_bb_minY(self):
        return dotty(self.json)[f'predictions.0.detection_boxes.{self._loop_num}.0']

    @property
    def loop_bb_minX(self):
        return dotty(self.json)[f'predictions.0.detection_boxes.{self._loop_num}.1']

    @property
    def loop_bb_maxY(self):
        return dotty(self.json)[f'predictions.0.detection_boxes.{self._loop_num}.2']

    @property
    def loop_bb_maxX(self):
        return dotty(self.json)[f'predictions.0.detection_boxes.{self._loop_num}.3']

    @property
    def pin_bb_minY(self):
        return dotty(self.json)[f'predictions.0.detection_boxes.{self._pin_num}.0']

    @property
    def pin_bb_minX(self):
        return dotty(self.json)[f'predictions.0.detection_boxes.{self._pin_num}.1']

    @property
    def pin_bb_maxY(self):
        return dotty(self.json)[f'predictions.0.detection_boxes.{self._pin_num}.2']

    @property
    def pin_bb_maxX(self):
        return dotty(self.json)[f'predictions.0.detection_boxes.{self._pin_num}.3']

    @property
    def loop_top_classification(self):
        return dotty(self.json)[
            f'predictions.0.detection_classes_as_text.{self._loop_num}'
        ]

    @property
    def pin_base_x(self):
        return dotty(self.json)[f'predictions.0.detection_boxes.{self._pin_num}.3']


class AutoMLMessageFactory(MessageFactory):
    def __init__(self):
        super().__init__('automl')

    def _parse_type_id(self, response: Any):
        return ResponseMessage.parse_type_id(response)


class AutoMLClientTransport(HttpClientTransport):
    """Overrides HttpClientTransport and handles message reading and writing"""

    def __init__(self, connection_name: str, url: str, config: dict = {}):
        super().__init__(
            connection_name,
            url,
            MessageResponseReader(),
            MessageRequestWriter(),
            config,
        )


@register_connection('automl')
class AutoMLClientConnection(ConnectionBase):
    """Overrides ConnectionBase and creates a GCP AutoML connection"""

    def __init__(
        self,
        connection_name: str,
        url: str,
        incoming_message_queue: IncomingMessageQueue,
        outgoing_message_queue: OutgoingMessageQueue,
        config: dict = {},
    ):
        super().__init__(
            connection_name,
            url,
            AutoMLClientTransport(connection_name, url, config),
            incoming_message_queue,
            outgoing_message_queue,
            AutoMLMessageFactory(),
            config,
        )
