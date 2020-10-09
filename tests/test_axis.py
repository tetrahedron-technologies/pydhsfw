from context import pydhsfw
import logging
import sys
import time
import signal
from typing import Any
from pydhsfw.processors import register_message_handler
from pydhsfw.axis import JpegReceiverGetRequestMessage, JpegReceiverImagePostRequestMessage
from pydhsfw.dhs import Dhs, DhsContext, DhsInit, DhsStart

_logger = logging.getLogger(__name__)

@register_message_handler('dhs_init')
def dhs_init(message:DhsInit, context:DhsContext):
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(funcName)s():%(lineno)d - %(message)s"
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S")

@register_message_handler('dhs_start')
def dhs_start(message:DhsStart, context:DhsContext):
    url = 'http://:7171'

    context.create_connection('axis_svr_conn', 'axis', url)
    context.get_connection('axis_svr_conn').connect()

    time.sleep(3)

@register_message_handler('axis_get_request')
def axis_image_request(message:JpegReceiverGetRequestMessage, context:DhsContext):
    _logger.info(message)
    
@register_message_handler('jpeg_receiver_image_post_request')
def axis_image_request(message:JpegReceiverImagePostRequestMessage, context:DhsContext):
    _logger.debug(message.file)

dhs = Dhs()
dhs.start()
sigs = {}
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)