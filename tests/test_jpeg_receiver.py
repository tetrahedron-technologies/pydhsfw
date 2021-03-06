# -*- coding: utf-8 -*-
import logging
import sys
import time
import signal
from pydhsfw.processors import register_message_handler
from pydhsfw.jpeg_receiver import JpegReceiverImagePostRequestMessage
from pydhsfw.dhs import Dhs, DhsContext, DhsInit, DhsStart

_logger = logging.getLogger(__name__)


@register_message_handler('dhs_init')
def dhs_init(message: DhsInit, context: DhsContext):
    logformat = (
        '[%(asctime)s] %(levelname)s:%(name)s:%(funcName)s():%(lineno)d - %(message)s'
    )
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format=logformat,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


@register_message_handler('dhs_start')
def dhs_start(message: DhsStart, context: DhsContext):
    url = 'http://:7171'

    context.create_connection('jpeg_receiver_conn', 'jpeg_receiver', url)
    context.get_connection('jpeg_receiver_conn').connect()

    time.sleep(3)


@register_message_handler('jpeg_receiver_image_post_request')
def axis_image_request(
    message: JpegReceiverImagePostRequestMessage, context: DhsContext
):
    _logger.debug(message.file)


dhs = Dhs()
dhs.start()
sigs = {}
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)
