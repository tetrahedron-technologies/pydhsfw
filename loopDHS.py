import logging
import signal
import sys
import yaml
import time
import io
from random import choice
from string import ascii_uppercase, digits

from pydhsfw.processors import  Context, register_message_handler
from pydhsfw.dhs import Dhs, DhsInit, DhsStart, DhsContext
from pydhsfw.dcss import DcssContext, DcssStoCSendClientType, DcssHtoSClientIsHardware, DcssStoHRegisterOperation, DcssStoHStartOperation, DcssHtoSOperationUpdate, DcssHtoSOperationCompleted, register_dcss_start_operation_handler
from pydhsfw.automl import AutoMLPredictRequest, AutoMLPredictResponse
from pydhsfw.jpeg_receiver import JpegReceiverImagePostRequestMessage

_logger = logging.getLogger(__name__)

@register_message_handler('dhs_init')
def dhs_init(message:DhsInit, context:DhsContext):

    loglevel = logging.INFO

    parser = message.parser

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

    args = parser.parse_args(message.args)

    #loglevel = logging.DEBUG

    #Logging setup. Will be able to change logging level later with config parameters.
    logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(funcName)s():%(lineno)d - %(message)s"
    logging.basicConfig(level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S")
    #Update log level for all registered log handlers.
    #for handler in logging.root.handlers:
    #    handler.setLevel(loglevel)

    conf_file = 'config/' + args.beamline + '.config'

    _logger.info("Initializing DHS")
    _logger.info(f"config file: {conf_file}")
    with open(conf_file, 'r') as f:
        conf = yaml.safe_load(f)
        dcss_host = conf['dcss']['host']
        dcss_port = conf['dcss']['port']
        automl_host = conf['loopdhs']['automl']['host']
        automl_port = conf['loopdhs']['automl']['port']
        jpeg_receiver_port = conf['loopdhs']['jpeg_receiver']['port']
        _logger.info(f"DCSS HOST: {dcss_host} PORT: {dcss_port}")
        _logger.info(f"AUTOML HOST: {automl_host} PORT: {automl_port}")

    dcss_url = 'dcss://' + dcss_host + ':' + str(dcss_port)
    automl_url = 'http://' + automl_host + ':' + str(automl_port)
    jpeg_receiver_url = 'http://localhost:' + str(jpeg_receiver_port)
    # merge values from command line and config file:
    context.state = {'DHS': args.dhs_name, 'dcss_url': dcss_url, 'automl_url': automl_url, 'jpeg_receiver_url': jpeg_receiver_url}

@register_message_handler('dhs_start')
def dhs_start(message:DhsStart, context:DhsContext):
    dcss_url = context.state['dcss_url']
    automl_url = context.state['automl_url']
    jpeg_receiver_url = context.state['jpeg_receiver_url']

    # Connect to DCSS
    context.create_connection('dcss_conn', 'dcss', dcss_url)
    context.get_connection('dcss_conn').connect()

    # Connect to GCP AutoML docker service
    context.create_connection('automl_conn', 'automl', automl_url, {'heartbeat_path': '/v1/models/default'})
    context.get_connection('automl_conn').connect()

    # Open a jpeg receiving port. Not sure this needs to be open all the time.
    # but Giles suggests that this is safe because unexpected data arrivign on jpeg_receiver_url
    # will be ignored if there is no activeOp to deal with it.
    context.create_connection('jpeg_receiver_conn', 'jpeg_receiver', jpeg_receiver_url)
    #context.get_connection('jpeg_receiver_conn').connect()

@register_message_handler('stoc_send_client_type')
def dcss_send_client_type(message:DcssStoCSendClientType, context:Context):
    context.get_connection('dcss_conn').send(DcssHtoSClientIsHardware(context.state['DHS']))

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
        context.get_connection('dcss_conn').send(DcssHtoSOperationUpdate(ao.operation_name, ao.operation_handle, "working on things"))
        context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(ao.operation_name, ao.operation_handle, "normal", "h1"))
    _logger.debug(f'Active operations post-completed={context.get_active_operations(message.operation_name)}')

@register_dcss_start_operation_handler('helloWorld2')
def hello_world_2(message:DcssStoHStartOperation, context:DcssContext):
    _logger.info("doing the stuff2")
    activeOps = context.get_active_operations(message.operation_name)
    _logger.debug(f'Active operations pre-completed={activeOps}')
    for ao in activeOps:
        context.get_connection('dcss_conn').send(DcssHtoSOperationUpdate(ao.operation_name, ao.operation_handle, "working on things2"))
        context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(ao.operation_name, ao.operation_handle, "normal", "h2"))
    _logger.debug(f'Active operations post-completed={context.get_active_operations(message.operation_name)}')

@register_dcss_start_operation_handler('predictOne')
def predict_one(message:DcssStoHStartOperation, context:DcssContext):
    """
    The operation is for testing AutoML. It reads a single image of a nylon loop from the tests directory and sends it to AutoML.
    """
    _logger.info(f'GOT: {message}')
    # I'm not sure why we loop through all active ops?
    activeOps = context.get_active_operations('predictOne')
    for ao in activeOps:
        context.get_connection('dcss_conn').send(DcssHtoSOperationUpdate(ao.operation_name, ao.operation_handle, "about to predict one test image"))
        image_key = "THE-TEST-IMAGE"
        filename = 'tests/loop_nylon.jpg'
        with io.open(filename, 'rb') as image_file:
            binary_image = image_file.read()
        context.get_connection('automl_conn').send(AutoMLPredictRequest(image_key, binary_image))
        # is the rest of this operation done elsewhere?

        # how to get return_msg?
        #context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(ao.operation_name, ao.operation_handle, "normal", return_msg))

@register_dcss_start_operation_handler('collectLoopImages')
def collect_loop_images(message:DcssStoHStartOperation, context:DcssContext):
    """
    This operation initiates the the jpeg receiver, informs DCSS to start_oscillation, as each jpeg image file is received it is processed
    and the AutoML results sent back as operation updates.

    DCSS may send a single arg <pinBaseSizeHint>, but I think we can ignore it.
    """
    _logger.info(f'GOT: {message}')
    activeOps = context.get_active_operations(message.operation_name)
    for ao in activeOps:
        # 1. Tell the http server to get ready to receive images
        # NOT NEEDED. WILL LEAVE PORT OPEN ALWAYS
        context.get_connection('jpeg_receiver_conn').connect()
        # 2. Send an operation update message to DCSS to trigger both sample rotation and axis server to send images.
        #    htos_operation_update collectLoopImages operation_handle start_oscillation
        context.get_connection('dcss_conn').send(DcssHtoSOperationUpdate(ao.operation_name, ao.operation_handle, "start_oscillation"))

        # I'm guessing everything below will happen in the AutoML response handler code:
        #
        # 3. As each image arrives forward it to AutoML for loop classification and bounding box determination.
        # 4. Format an operation update for each image and send to DCSS
        #    for error:
        #    htos_operation_update collectLoopImages operation_handle LOOP_INFO <index> failed <error_message>
        #    for success:
        #    htos_operation_update collectLoopImages operation_handle LOOP_INFO <index> normal tipX tipY pinBaseX fiberWidth loopWidth boxMinX boxMaxX boxMinY boxMaxY loopWidthX isMicroMount
        # 5. Listen for some global flag/signal (set by the stopCollectLoopImages operation) that operation should stop processing images
        #    and then send an operation complete message to DCSS
        #    for error:
        #    htos_operation_completed collectLoopImages operation_handle aborted
        #    for success:
        #    htos_operation_completed collectLoopImages operation_handle normal done
        # 6. Close the jpegreceiver
        # NOT NEEDED. WILL LEAVE PORT OPEN ALWAYS
        
@register_dcss_start_operation_handler('getLoopTip')
def get_loop_tip(message:DcssStoHStartOperation, context:DcssContext):
    """
    This operation should return the position of the right (or left) most point of a loop.
    This operation takes a single optional integer arg <ifaskPinPosFlag>
    
    1. htos_operation_completed getLoopTip operation_handle normal tipX tipY  (when ifaskPinPosFlag = 0)
       or:
       htos_operation_completed getLoopTip operation_handle normal tipX tipY pinBaseX (when ifaskPinPosFlag = 1)
    2. htos_operation_completed getLoopTip operation_handle error TipNotInView +/-

    """
    _logger.info(f'GOT: {message}')
    # need to confirm that pinBaseX is the same as PinPos in imgCentering.cc
    #
    # 1. Request single jpeg image from axis video server
    # 2. Send to AutoML
    # 3. Format results and send results bacxk to DCSS

@register_dcss_start_operation_handler('getLoopInfo')
def get_loop_info(message:DcssStoHStartOperation, context:DcssContext):
    """
    This operation should return full suite of info about a single image.

    DCSS may send a single arg pinBaseSizeHint, but I think we can ignore it.
    """
    _logger.info(f'GOT: {message}')
    # 1. Grab single image from Axis Video server
    #    on error:
    #    htos_operation_completed getLoopInfo operation_handle error failed to get image
    # 2. Send to AutoML
    # 3. Format AutoML results and send info back to DCSS
    #    on error:
    #    htos_operation_completed getLoopInfo operation_handle failed <error_message>
    #    on success:
    #    htos_operation_completed getLoopInfo operation_handle normal tipX tipY pinBaseX fiberWidth loopWidth boxMinX boxMaxX boxMinY boxMaxY loopWidthX isMicroMount

@register_dcss_start_operation_handler('stopCollectLoopImages')
def stop_collect_loop_images(message:DcssStoHStartOperation, context:DcssContext):
    """
    This operation should set a global flag to signal collectLoopImages to stop and optionally to shutdown the jpeg receiver.
    """
    _logger.info(f'GOT: {message}')

    # 1. Set global stop flag
    # 2. Shutdown jpeg receiver
    context.get_connection('jpeg_receiver_conn').disconnect()
    # 3. Send update message to DCSS
    #    htos_operation_completed stopCollectLoopImages operation_handle normal flag set

@register_dcss_start_operation_handler('reboxLoopImage')
def rebox_loop_image(message:DcssStoHStartOperation, context:DcssContext):
    """
    This operation is used to more accurately define the loop bounding box.

    Parameters:
    index (int):
    start (double):
    end (double):
    """
    _logger.info(f'GOT: {message}')
    # need to figure this out.

@register_message_handler('automl_predict_response')
def automl_predict_response(message:AutoMLPredictResponse, context:DcssContext):
    """
    This handler is hardcoded to accompany the predictOne operation
    """

    activeOps = context.get_active_operations('predictOne')
    for ao in activeOps:
        predict_one_msg = ' '.join([str(message.image_key), str(message.top_result), str(message.top_bb[0]), str(message.top_bb[1]), str(message.top_bb[2]), str(message.top_bb[3]), message.top_classification])
        _logger.info(f'AUTOML: {predict_one_msg}')
        context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(ao.operation_name, ao.operation_handle, "normal", predict_one_msg))

    activeOps = context.get_active_operations('collectLoopImages')
    for ao in activeOps:
        # need to increment index for each image we receive
        index = 1
        status = 'normal'
        tipX = message.top_bb[2]
        tipY = (message.top_bb[3] - message.top_bb[1])/2
        pinBaseX = 0.111
        fiberWidth = 0.222
        loopWidth = (message.top_bb[3] - message.top_bb[1])
        boxMinX = message.top_bb[0]
        boxMaxX = message.top_bb[2]
        boxMinY = message.top_bb[3]
        boxMaxY = message.top_bb[1]
        loopWidthX = (message.top_bb[2] - message.top_bb[0])
        isMicroMount = 0
        # I would like to format these numbers so they aren't so long.
        collect_loop_images_msg = ' '.join(map(str,['LOOP INFO', index, status, tipX, tipY, pinBaseX, fiberWidth, loopWidth, boxMinX, boxMaxX, boxMinY, boxMaxY, loopWidthX, isMicroMount]))
        _logger.info(f'FOR DCSS: {collect_loop_images_msg}')
        context.get_connection('dcss_conn').send(DcssHtoSOperationUpdate(ao.operation_name,ao.operation_handle,collect_loop_images_msg))

@register_message_handler('jpeg_receiver_image_post_request')
def axis_image_request(message:JpegReceiverImagePostRequestMessage, context:DhsContext):
    _logger.debug(message.file)
    # generate key
    image_key = ''.join(choice(ascii_uppercase + digits) for i in range(12))
    # send 
    context.get_connection('automl_conn').send(AutoMLPredictRequest(image_key, message.file))

    #    for error:
    #    htos_operation_update collectLoopImages operation_handle LOOP_INFO <index> failed <error_message>
    #    for success:
    #    htos_operation_update collectLoopImages operation_handle LOOP_INFO <index> normal tipX tipY pinBaseX fiberWidth loopWidth boxMinX boxMaxX boxMinY boxMaxY loopWidthX isMicroMount
    #return_msg = "LOOP_INFO 1 normal 0.5 0.5 0.4 0.1 0.22 1 2 3 4 5 6"
    # so I guess this is where we end up if an image arrives on teh jpeg_receiver_port.
    #activeOps = context.get_active_operations('collectLoopImages')
    #for ao in activeOps:
    #    context.get_connection('dcss_conn').send(DcssHtoSOperationUpdate(ao.operation_name,ao.operation_handle, return_msg))

dhs = Dhs()
dhs.start()
sigs = {}
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)