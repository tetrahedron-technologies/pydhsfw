import logging
import signal
from pydhsfw.processors import  Context, register_message_handler
from pydhsfw.dcss import DcssStoCSendClientType, DcssHtoSClientIsHardware, DcssStoHRegisterOperation
from pydhsfw.dhs import Dhs, DhsInit

_logger = logging.getLogger(__name__)

@register_message_handler('dhs_init')
def dhs_init(message:DhsInit, context:Context):
    #print("Initializing DHS")
    _logger.info("Initializing DHS")

    url = 'dcss://localhost:14242'
    context.create_connection('dcss', url)
    context.get_connection('dcss').connect()

@register_message_handler('stoc_send_client_type')
def dcss_send_client_type(message:DcssStoCSendClientType, context:Context):
    context.get_connection('dcss').send(DcssHtoSClientIsHardware('loopDHS'))

@register_message_handler('stoh_register_operation')
def dcss_reg_operation(message:DcssStoHRegisterOperation, context:Context):
    #print(f'Handling message {message}')
    _logger.info(f"Handling message {message}")

dhs = Dhs()
dhs.start()
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)