import logging
import signal
import sys
import yaml
from pydhsfw.processors import  Context, register_message_handler
from pydhsfw.dcss import DcssContext, DcssStoCSendClientType, DcssHtoSClientIsHardware, DcssStoHRegisterOperation, DcssStoHStartOperation, DcssHtoSOperationUpdate, DcssHtoSOperationCompleted, register_dcss_start_operation_handler
from pydhsfw.dhs import Dhs, DhsInit, DhsStart, DhsContext

_logger = logging.getLogger(__name__)

@register_message_handler('dhs_init')
def dhs_init(message:DhsInit, context:DhsContext):

    parser = message.parser
    #print(f"parser: {parser}")
    #parser = argparse.ArgumentParser(description="DHS Distributed Hardware Server")
    parser.add_argument(
        "--version",
        action="version",
        version="0.1")
        #version="pyDHS {ver}".format(ver=__version__))
    #parser.add_argument(
    #    dest="beamline",
    #    help="Beamline Name (e.g. BL-831)",
    #    metavar="Beamline")
    #parser.add_argument(
    #    dest="dhs_name",
    #    help="DHS Name (e.g. loop or chain or detector)",
    #    metavar="DHS Name")
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
    parser.add_argument(
        "-c",
        "--config",
        dest="config_file",
        help="pull settings from config file",
        metavar="config_file")
    
    args = parser.parse_args(message.args)
    #print(args)
    # I'm not sure how to set a default logging level in argparse so will try this
    #if args.loglevel == None:
    #    loglevel = logging.INFO
    #else:
    #    loglevel = args.loglevel

    loglevel = logging.DEBUG

    #Logging setup. Will be able to change logging level later with config parameters.
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(funcName)s():%(lineno)d - %(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S")
    #Update log level for all registered log handlers.
    #for handler in logging.root.handlers:
    #    handler.setLevel(loglevel)

    conf_file = 'config/bl831.config'

    _logger.info("Initializing DHS")
    _logger.info(f"config file: {conf_file}")
    with open(conf_file, 'r') as f:
        conf = yaml.safe_load(f)
        dcss_host = conf['dcss']['host']
        dcss_port = conf['dcss']['port']
        _logger.info(f"DCSS HOST: {dcss_host} PORT: {dcss_port}")
    
    url = 'dcss://' + dcss_host + ':' + str(dcss_port)
    context.state = {'url':url}

@register_message_handler('dhs_start')
def dhs_start(message:DhsStart, context:DhsContext):
    url = context.state['url']

    context.create_connection('dcss', url)
    context.get_connection('dcss').connect()

@register_message_handler('stoc_send_client_type')
def dcss_send_client_type(message:DcssStoCSendClientType, context:Context):
    context.get_dhs_state['bob'] = "ya!"
    context.get_connection('dcss').send(DcssHtoSClientIsHardware('loopDHS'))

@register_message_handler('stoh_register_operation')
def dcss_reg_operation(message:DcssStoHRegisterOperation, context:Context):
    #print(f'Handling message {message}')
    _logger.info(f"Handling message {message}")

@register_message_handler('stoh_start_operation')
def dcss_start_operation(message:DcssStoHStartOperation, context:Context):
    _logger.info(f"GOT: {message}")
    op = message.operation_name
    opid = message.operation_handle
    _logger.info(f"operation={op}, handle={opid}")

@register_dcss_start_operation_handler('helloWorld')
def hello_world_1(message:DcssStoHStartOperation, context:DcssContext):
    _logger.info("doing the stuff1")
    activeOps = context.get_active_operations(message.operation_name)
    _logger.debug(f'Active operations pre-completed={activeOps}')
    for ao in activeOps:
        context.get_connection('dcss').send(DcssHtoSOperationUpdate(ao.operation_name, ao.operation_handle, "working on things"))
        context.get_connection('dcss').send(DcssHtoSOperationCompleted(ao.operation_name, ao.operation_handle, "normal", "h1"))
    _logger.debug(f'Active operations post-completed={activeOps}')


@register_dcss_start_operation_handler('helloWorld2')
def hello_world_2(message:DcssStoHStartOperation, context:DcssContext):
    _logger.info("doing the stuff2")
    activeOps = context.get_active_operations(message.operation_name)
    _logger.debug(f'Active operations pre-completed={activeOps}')
    for ao in activeOps:
        context.get_connection('dcss').send(DcssHtoSOperationUpdate(ao.operation_name, ao.operation_handle, "working on things2"))
        context.get_connection('dcss').send(DcssHtoSOperationCompleted(ao.operation_name, ao.operation_handle, "normal", "h2"))
    _logger.debug(f'Active operations post-completed={activeOps}')

dhs = Dhs()
dhs.start()
sigs = {}
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)