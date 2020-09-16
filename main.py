from signal import signal, SIGINT, SIGTERM
from pydhsfw.processors import  Context, register_message_handler
from pydhsfw.dcss import DcssStoCSendClientType, DcssCtoSClientIsHardware, DcssStoHRegisterOperation
from pydhsfw.dhs import Dhs, DhsInit


@register_message_handler('dhs_init')
def dhs_init(message:DhsInit, context:Context):
    print("Initializing DHS")

    url = 'dcss://localhost:14242'
    context.create_connection('dcss', url)
    context.get_connection('dcss').connect()

@register_message_handler('stoc_send_client_type')
def dcss_send_client_type(message:DcssStoCSendClientType, context:Context):
    context.get_connection('dcss').send(DcssCtoSClientIsHardware('loopDHS'))

@register_message_handler('stoh_register_operation')
def dcss_reg_operation(message:DcssStoHRegisterOperation, context:Context):
    print(f'Handling message {message}')

        
dhs = Dhs().start()

def handler(signal_received, frame):
    # Handle any cleanup here
    print('SIGINT or CTRL-C detected. Exiting gracefully')
    dhs.shutdown()

if __name__ == '__main__':
    # Tell Python to run the handler() function when SIGINT is recieved
    signal(SIGINT, handler)
    signal(SIGTERM, handler)

dhs.wait()