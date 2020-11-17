# -*- coding: utf-8 -*-
import logging
import sys
import time
import signal
from pydhsfw.processors import register_message_handler
from pydhsfw.axis import AxisImageRequestMessage, AxisImageResponseMessage
from pydhsfw.dhs import Dhs, DhsContext, DhsInit, DhsStart

_logger = logging.getLogger(__name__)


@register_message_handler('dhs_init')
def dhs_init(message: DhsInit, context: DhsContext):
    logformat = (
        '[%(asctime)s] %(levelname)s:%(name)s:%(funcName)s():%(lineno)d - %(message)s'
    )
    logging.basicConfig(
        level=logging.DEBUG,
        stream=sys.stdout,
        format=logformat,
        datefmt='%Y-%m-%d %H:%M:%S',
    )


@register_message_handler('dhs_start')
def dhs_start(message: DhsStart, context: DhsContext):
    url = 'http://141.211.27.126'
    conn = context.create_connection(
        'axis_conn',
        'axis',
        url,
        {
            'heartbeat_path': '/axis-cgi/jpg/image.cgi?resolution=640x360&text=0&clock=0&date=0'
        },
    )
    conn.connect()

    time.sleep(3)

    conn.send(AxisImageRequestMessage())


@register_message_handler('axis_image_response')
def axis_image_response(message: AxisImageResponseMessage, context: DhsContext):
    _logger.info(f'Received image, {message.file_length} bytes')


dhs = Dhs()
dhs.start()
sigs = {}
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)
