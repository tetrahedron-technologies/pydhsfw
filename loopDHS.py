import signal
from pydhsfw.processors import  Context, register_message_handler
from pydhsfw.dcss import DcssStoCSendClientType, DcssHtoSClientIsHardware, DcssStoHRegisterOperation
from pydhsfw.dhs import Dhs, DhsInit


@register_message_handler('dhs_init')
def dhs_init(message:DhsInit, context:Context):
    print("Initializing Loop DHS")

    parser = message.get_parser()
    #print(f"parser: {parser}")
    #parser = argparse.ArgumentParser(description="DHS Distributed Hardware Server")
    parser.add_argument(
        "--version",
        action="version",
        version="0.1")
        #version="pyDHS {ver}".format(ver=__version__))
    parser.add_argument(
        dest="beamline",
        help="Beamline Name (e.g. BL-831)",
        metavar="Beamline")
    parser.add_argument(
        dest="dhs_name",
        help="DHS Name (e.g. loop or chain or detector)",
        metavar="DHS Name")
    parser.add_argument(
        "-v",
        "--verbose",
        dest="loglevel",
        help="set loglevel to INFO",
        action="store_const",
        const="TEST")
        # comment out until logging is configured
        #const=logging.INFO)
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const="TEST")
        # comment out until logging is configured
        #const=logging.DEBUG)

    parsed_config_stuff = parser.parse_args(message.get_args())

    url = 'dcss://localhost:14242'
    context.create_connection('dcss', url)
    context.get_connection('dcss').connect()

@register_message_handler('stoc_send_client_type')
def dcss_send_client_type(message:DcssStoCSendClientType, context:Context):
    context.get_connection('dcss').send(DcssHtoSClientIsHardware('loopDHS'))

@register_message_handler('stoh_register_operation')
def dcss_reg_operation(message:DcssStoHRegisterOperation, context:Context):
    print(f'Handling message {message}')

        
dhs = Dhs()
dhs.start()
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)