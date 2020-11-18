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
    """Formats a json data package and sends to the Google AutoML server for prediction.

    As per the Google documentation the image must be base64 encoded.

    Note:
        Google claims there are a number of `parameters <https://cloud.google.com/automl/docs/reference/rest/v1/projects.locations.models/predict>`_ that can be passed in with this json package.
        However, I have had no luck in getting this working with our edge-deployed model.
    """

    def __init__(self, key: str, image: bytes):

        encoded_image = base64.b64encode(image).decode('utf-8')

        instances = {
            'instances': [{'image_bytes': {'b64': str(encoded_image)}, 'key': key}],
            'params': [{'max_bounding_box_count': '10'}],
        }

        super().__init__('/v1/models/default:predict', json=instances)


@register_message('automl_predict_response', 'automl')
class AutoMLPredictResponse(JsonResponseMessage):
    """Parses the response json file and stores various values from the top hit in class properties."""

    def __init__(self, response):
        super().__init__(response)

    def get_score(self, n: int) -> float:
        """Returns the AutoML inference score for the Nth object in a sorted results list"""
        # _logger.info(f'AutoMLPredictResponse function called with {n}')
        return dotty(self.json)[f'predictions.0.detection_scores.{n}']

    @property
    def image_key(self):
        """str: This is the unique key associated with this particular prediction request."""
        return dotty(self.json)['predictions.0.key']

    @property
    def top_score(self):
        """float: This is the highest AutoML score from this prediction request (0.0-1.0)"""
        return dotty(self.json)['predictions.0.detection_scores.0']

    @property
    def top_bb(self):
        """:obj:`list` of :obj:`float`: Bounding box returned from AutoML containing minY, minX, maxY, maxX

        The values returned are fractional coordinates with the origin in upper left corner of image::

            (0,0)----------------------> (1,0) X-axis (horizontal)
            |
            |
            |
            |
            v
            (0,1) Y-axis (vertical)

        """
        return dotty(self.json)['predictions.0.detection_boxes.0']

    @property
    def bb_minY(self):
        """float: The minimum vertical coordinate for the bounding box. i.e. the top of the bounding box."""
        return dotty(self.json)['predictions.0.detection_boxes.0.0']

    @property
    def bb_minX(self):
        """float: The minimum horizontal coordinate for the bounding box. i.e. the left side of the bounding box."""
        return dotty(self.json)['predictions.0.detection_boxes.0.1']

    @property
    def bb_maxY(self):
        """float: The maximum vertical coordinate for the bounding box. i.e. the bottom of the bounding box."""
        return dotty(self.json)['predictions.0.detection_boxes.0.2']

    @property
    def bb_maxX(self):
        """float: The maximum horizontal coordinate for the bounding box. i.e. the right side of the bounding box."""
        return dotty(self.json)['predictions.0.detection_boxes.0.3']

    @property
    def top_classification(self):
        """str: The text value associated with the top prediction.

        This depends on what type of AutoML model you are using and how many objects it has been trained to detect.

        Note:
            For our implementaion of the loopDHS at beamline 8.3.1 we have an AutoML model trained to detect **nylon** and **mitegen** loops as well as **pins**.
        """
        return dotty(self.json)['predictions.0.detection_classes_as_text.0']


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
