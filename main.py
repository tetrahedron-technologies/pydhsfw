import logging
import signal
import sys
import yaml
from pydhsfw.processors import  Context, register_message_handler
from pydhsfw.dcss import DcssStoCSendClientType, DcssHtoSClientIsHardware, DcssStoHRegisterOperation
from pydhsfw.dhs import Dhs, DhsInit

_logger = logging.getLogger(__name__)

@register_message_handler('dhs_init')
def dhs_init(message:DhsInit, context:Context):

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
        const=logging.INFO)
    parser.add_argument(
        "-vv",
        "--very-verbose",
        dest="loglevel",
        help="set loglevel to DEBUG",
        action="store_const",
        const=logging.DEBUG)

    args = parser.parse_args(message.get_args())
    #print(args)
    # I'm not sure how to set a default logging level in argparse so will try this
    if args.loglevel == None:
        loglevel = logging.INFO
    else:
        loglevel = args.loglevel

    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(lineno)d - %(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S")

    _logger.info("Initializing DHS")
    conf_file = message.get_conf_file()
    _logger.info(f"log file: {conf_file}")
    with open(conf_file, 'r') as f:
        conf = yaml.safe_load(f)
        dcss_host = conf['dcss']['host']
        dcss_port = conf['dcss']['port']
        _logger.info(f"DCSS HOST: {dcss_host} PORT: {dcss_port}")

    url = 'dcss://' + dcss_host + ':' + str(dcss_port)
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