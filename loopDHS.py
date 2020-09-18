import logging
import signal
import sys
from pydhsfw.processors import  Context, register_message_handler
#from pydhsfw.dcss import DcssStoCSendClientType, DcssHtoSClientIsHardware, DcssStoHRegisterOperation
from pydhsfw.dcss import *
from pydhsfw.dhs import Dhs, DhsInit

_logger = logging.getLogger(__name__)


class loopDHSState():
    def __init__(self, loop_dhs_state_mgr):
        self._dhs_state_mgr = loop_dhs_state_mgr
        self._state = None
        self._operation_name = None
        self._operation_handle = None
        self._operation_args = None

    def get_operation_name(self)->str:
        return self._operation_name

    def set_operation_name(self, opname)->str:
        self._operation_name = opname

    def get_operation_handle(self)->float:
        return self._operation_handle

    def set_operation_handle(self, opid)->float:
        self._operation_handle = opid

    def get_operation_args(self)->str:
        return self._operation_args

    def set_operation_args(self, opargs)->str:
        self._operation_args = opargs


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
    url = 'dcss://localhost:14242'
    context.create_connection('dcss', url)
    context.get_connection('dcss').connect()

@register_message_handler('stoc_send_client_type')
def dcss_send_client_type(message:DcssStoCSendClientType, context:Context):
    context.get_connection('dcss').send(DcssHtoSClientIsHardware('loopDHS'))

@register_message_handler('stoh_register_operation')
def dcss_reg_operation(message:DcssStoHRegisterOperation, context:Context):
    _logger.info(f"Handling message {message}")

@register_message_handler('stoh_start_operation')
def dcss_start_operation(message:DcssStoHStartOperation, context:Context):
    _logger.info(f"GOT: {message}")
    op = message.get_operation_name()
    opid = message.get_operation_handle()
    _logger.info(f"operation: {op}")
    
    if op == "helloWorld":
        context.get_connection('dcss').send(DcssHtoSOperationUpdate(op, opid, "working on things"))
        res = hello_world_1()
    elif op == "helloWorld2":
        res = hello_world_2()

    _logger.info(f"RESULT: {res}")
    context.get_connection('dcss').send(DcssHtoSOperationCompleted(op, opid, "normal", res))

def hello_world_1():
    _logger.info("doing the stuff1")
    # how do I call DcssHtoSOperationUpdate?
    result = "HELLO WORLD 1"
    return result

def hello_world_2():
    _logger.info("doing the stuff2")
    result = "HELLO WORLD 2"
    return result



dhs = Dhs()
dhs.start()
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)