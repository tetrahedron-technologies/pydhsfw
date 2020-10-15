import os
import logging
import coloredlogs
import verboselogs
import signal
import sys
import yaml
import time
import io
import glob
import re
import cv2
import math
from random import choice
#from string import ascii_uppercase, digits
from dotty_dict import dotty as dot

from pydhsfw.processors import  Context, register_message_handler
from pydhsfw.dhs import Dhs, DhsInit, DhsStart, DhsContext
from pydhsfw.dcss import DcssContext, DcssStoCSendClientType, DcssHtoSClientIsHardware, DcssStoHRegisterOperation, DcssStoHStartOperation, DcssHtoSOperationUpdate, DcssHtoSOperationCompleted, register_dcss_start_operation_handler
from pydhsfw.automl import AutoMLPredictRequest, AutoMLPredictResponse
from pydhsfw.jpeg_receiver import JpegReceiverImagePostRequestMessage

#_logger = logging.getLogger(__name__)
_logger = verboselogs.VerboseLogger('loopDHS.py')
#_logger.addHandler(logging.StreamHandler())

# add DHS-specific class to hold a group of jpeg images in memory.
class LoopImageSet():
    def __init__(self):
        self.images = []
        self.results = []
        self._number_of_images = None

    def add_image(self, image:bytes):
        """Add a jpeg image to the list of images"""
        self.images.append(image)
        self._number_of_images = len(self.images)

    def add_results(self, result:list):
        """Add the AutoML results to a list for use in reboxLoopImage"""
        self.results.append(result)

    @property
    def number_of_images(self) -> int:
        """Get the number of images in the image list"""
        return self._number_of_images

@register_message_handler('dhs_init')
def dhs_init(message:DhsInit, context:DhsContext):

    parser = message.parser

    parser.add_argument(
        '--version',
        action='version',
        version='0.1')
        #version='pyDHS {ver}'.format(ver=__version__))
    parser.add_argument(
        dest='beamline',
        help='Beamline Name (e.g. BL-831)',
        metavar='Beamline')
    parser.add_argument(
        dest='dhs_name',
        help='DHS Name (e.g. loop or chain or detector)',
        metavar='DHS Name')
    parser.add_argument(
        '-v',
        '--verbose',
        dest='verbosity',
        help='set chattiness of logging',
        action='count',
        default=0)


    args = parser.parse_args(message.args)

    # Not sure this is working. arg gets parsed but _logger never changes level. need to 
    # set a second varable loglevel and pass it to teh coloredlogs installer below.
    if args.verbosity >= 4:
        _logger.setLevel(logging.SPAM)
        loglevel = 5
    elif args.verbosity >= 3:
        _logger.setLevel(logging.DEBUG)
        loglevel = 10
    elif args.verbosity >= 2:
        _logger.setLevel(logging.VERBOSE)
        loglevel = 15
    elif args.verbosity >= 1:
        _logger.setLevel(logging.NOTICE)
        loglevel = 25
    elif args.verbosity <= 0:
        _logger.setLevel(logging.WARNING)
        loglevel = 30


    # By default the install() function installs a handler on the root logger,
    # this means that log messages from your code and log messages from the
    # libraries that you use will all show up on the terminal.
    #coloredlogs.install(level='DEBUG')
    
    # If you don't want to see log messages from libraries, you can pass a
    # specific logger object to the install() function. In this case only log
    # messages originating from that logger will show up on the terminal.
    #coloredlogs.install(level='DEBUG', logger=logger)


    coloredlogs.install(level=loglevel,fmt='%(asctime)s,%(msecs)03d %(hostname)s %(name)s-[%(funcName)s():%(lineno)d] %(levelname)s %(message)s')


    # levels supposed to be availbe with verboselogs module
    #  5 SPAM
    # 10 DEBUG
    # 15 VERBOSE
    # 20 INFO
    # 25 NOTICE
    # 30 WARNING
    # 35 SUCCESS
    # 40 ERROR
    # 50 CRITICAL
    # _logger.spam("this is a spam message")
    # _logger.debug("this is a debugging message")
    # _logger.verbose("this is a verbose message")
    # _logger.info("this is an informational message")
    # _logger.notice("this is a notice message")
    # _logger.warning("this is a warning message")
    # _logger.success("this is a success message")
    # _logger.error("this is an error message")
    # _logger.critical("this is a critical message")

   # logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(funcName)s():%(lineno)d - %(message)s"
   # logging.basicConfig(level='INFO', stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S")
    #Update log level for all registered log handlers.
    #for handler in logging.root.handlers:
    #    handler.setLevel(loglevel)

    conf_file = 'config/' + args.beamline + '.config'

    _logger.success('=============================================')
    _logger.success('Initializing Loop DHS')
    l = logging.getLevelName(_logger.getEffectiveLevel())
    _logger.success(f'Logging level is set to: {l}')
    _logger.success(f'config file: {conf_file}')
    with open(conf_file, 'r') as f:
        yconf = yaml.safe_load(f)
        conf = dot(yconf)
        dcss_host = conf['dcss.host']
        dcss_port = conf['dcss.port']
        automl_host = conf['loopdhs.automl.host']
        automl_port = conf['loopdhs.automl.port']
        axis_host = conf['loopdhs.axis.host']
        axis_port = conf['loopdhs.axis.port']
        jpeg_receiver_port = conf['loopdhs.jpeg_receiver.port']
        jpeg_save_dir = conf['loopdhs.image_save_dir']
        _logger.success(f'DCSS HOST: {dcss_host} PORT: {dcss_port}')
        _logger.success(f'AUTOML HOST: {automl_host} PORT: {automl_port}')
        _logger.success(f'JPEG RECEIVER PORT: {jpeg_receiver_port}')
        _logger.success(f'AXIS HOST: {axis_host} PORT: {axis_port}')
    _logger.success('=============================================')

    dcss_url = 'dcss://' + dcss_host + ':' + str(dcss_port)
    automl_url = 'http://' + automl_host + ':' + str(automl_port)
    jpeg_receiver_url = 'http://localhost:' + str(jpeg_receiver_port)
    axis_url = ''.join(map(str,['http://',axis_host,':',axis_port])) 
    # merge values from command line and config file:
    context.state = {
        'DHS': args.dhs_name,
        'dcss_url': dcss_url,
        'automl_url': automl_url,
        'jpeg_receiver_url': jpeg_receiver_url,
        'axis_url': axis_url,
        'jpeg_save_dir': jpeg_save_dir
    }
    context.gStopJpegStream = 0
    

    if not os.path.exists(jpeg_save_dir):
        os.makedirs(''.join([jpeg_save_dir,'bboxes']))
    else:
         empty_jpeg_dir()

@register_message_handler('dhs_start')
def dhs_start(message:DhsStart, context:DhsContext):
    dcss_url = context.state['dcss_url']
    automl_url = context.state['automl_url']
    jpeg_receiver_url = context.state['jpeg_receiver_url']
    axis_url = context.state['axis_url']

    # Connect to DCSS
    context.create_connection('dcss_conn', 'dcss', dcss_url)
    context.get_connection('dcss_conn').connect()

    # Connect to GCP AutoML docker service
    context.create_connection('automl_conn', 'automl', automl_url, {'heartbeat_path': '/v1/models/default'})
    context.get_connection('automl_conn').connect()

    # Connect to an AXIS Video Server

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
    _logger.info(f'REGISTER: {message}')

@register_message_handler('stoh_start_operation')
def dcss_start_operation(message:DcssStoHStartOperation, context:Context):
    _logger.info(f"FROM DCSS: {message}")
    op = message.operation_name
    opid = message.operation_handle
    _logger.info(f"OPERATION: {op}, HANDLE: {opid}")

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
    _logger.info(f'FROM DCSS: {message}')
    activeOps = context.get_active_operations(message.operation_name)
    _logger.info(f'Active operations pre-completed={activeOps}')
    context.get_connection('dcss_conn').send(DcssHtoSOperationUpdate(message.operation_name, message.operation_handle, "about to predict one test image"))
    image_key = "THE-TEST-IMAGE"
    filename = 'tests/loop_nylon.jpg'
    with io.open(filename, 'rb') as image_file:
        binary_image = image_file.read()
    context.get_connection('automl_conn').send(AutoMLPredictRequest(image_key, binary_image))

@register_dcss_start_operation_handler('collectLoopImages')
def collect_loop_images(message:DcssStoHStartOperation, context:DcssContext):
    """
    This operation initiates the the jpeg receiver, informs DCSS to start_oscillation, as each jpeg image file is received it is processed
    and the AutoML results sent back as operation updates.

    DCSS may send a single arg <pinBaseSizeHint>, but I think we can ignore it.
    """
    _logger.info(f'FROM DCSS: {message}')

    # 1. Instaniate LoopImageSet
    context.jpegs = None
    context.jpegs = LoopImageSet()

    # 2. Clear the stop flag. change to True Flas
    context.gStopJpegStream = 0
    
    # 3. Open jpeg_receiver_port
    context.get_connection('jpeg_receiver_conn').connect()
    # might take some time. for now we will just wait a couple seconds.
    #time.sleep(2)

    # 4. Send an operation update message to DCSS to trigger both sample rotation and axis server to send images.
    #    htos_operation_update collectLoopImages operation_handle start_oscillation
    context.get_connection('dcss_conn').send(DcssHtoSOperationUpdate(message.operation_name, message.operation_handle, "start_oscillation"))

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
    _logger.info(f'FROM DCSS: {message}')
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
    _logger.info(f'FROM DCSS: {message}')
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
    _logger.info(f'FROM DCSS: {message}')

    # 1. Set global stop flag
    context.gStopJpegStream = 1

    # 2. Shutdown jpeg receiver
    context.get_connection('jpeg_receiver_conn').disconnect()

    # 3. Send operation completed message to DCSS
    context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(message.operation_name,message.operation_handle,'normal','flag set'))

    # 4. If there is an active collectLoopImages operation send a completed message.
    activeOps = context.get_active_operations()
    for ao in activeOps:
        if ao.operation_name == 'collectLoopImages':
            context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(ao.operation_name,ao.operation_handle,'normal','done'))

@register_dcss_start_operation_handler('reboxLoopImage')
def rebox_loop_image(message:DcssStoHStartOperation, context:DcssContext):
    """
    This operation is used to more accurately define the loop bounding box. I'm not sure of it's use with AutoML loop prediction, but it was important for teh original edge detection AI developed at SSRL.

    Parameters:
    index (int): which image we want to inspect
    start (double): X position start. Used for bracket. We will not use this.
    end (double): X position end. Used for bracket. We will not use this.

    e.g. reboxLoopImage 1.4 43 0.517685 0.563561

    Returns:
    returnIndex
    resultMinY
    resultMaxY
    (resultMaxY - resultMinY)

    """
    _logger.info(f'FROM DCSS: {message}')
    request_img = int(message.operation_args[0])
    stuff = context.jpegs.results[request_img]
    _logger.info(f'REQUEST IMAGE: {request_img} RESULTS: {stuff}')
    index = stuff.split(' ')[1]
    tipY = stuff.split(' ')[4]
    boxMaxY = stuff.split(' ')[11]
    boxMinY = stuff.split(' ')[10]
    return_msg = ' '.join([index, boxMinY, boxMaxY, tipY])
    context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(message.operation_name,message.operation_handle,'normal', return_msg))

@register_message_handler('automl_predict_response')
def automl_predict_response(message:AutoMLPredictResponse, context:DcssContext):
    """
    This handler will deal with stuff coming back from AutoML
    """

    # ==============================================================
    activeOps = context.get_active_operations()
    _logger.debug(f'Active operations pre-completed={activeOps}')
    # ==============================================================

    for ao in activeOps:
        if ao.operation_name == 'predictOne':
            predict_one_msg = ' '.join(map(str,[message.image_key, message.top_result, message.top_bb[0], message.top_bb[1], message.top_bb[2], message.top_bb[3], message.top_classification]))
            _logger.info(f'AUTOML: {predict_one_msg}')
            context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(ao.operation_name, ao.operation_handle, "normal", predict_one_msg))
        elif ao.operation_name == 'collectLoopImages' and not context.gStopJpegStream:
            # massage AutoML results for consumption by DCSS loopFast operation
            # this index method is NOT working.
            index = message.image_key.split(':')[2]
            status = 'normal'
            tipX = round(message.bb_maxX,5)
            tipY = round((message.bb_maxY - message.bb_minY)/2,5)
            pinBaseX = 0.111 # will add once we have AutoML model that recognizes pins
            fiberWidth = 0.222 # not sure we can or need to support this.
            loopWidth = round((message.bb_maxY - message.bb_minY),5)
            boxMinX = round(message.bb_minX,5)
            boxMaxX = round(message.bb_maxX,5)
            boxMinY = round(message.bb_minY,5)
            boxMaxY = round(message.bb_maxY,5)
            loopWidthX = round((message.bb_maxX - message.bb_minX),5)
            if message.top_classification == 'mitegen':
                isMicroMount = 1
            else:
                isMicroMount = 0
            loopClass = message.top_classification

            # Draw the AutoML bounding box
            UL = [message.bb_minX,message.bb_minY]
            LR = [message.bb_maxX,message.bb_maxY]
            _logger.info(f'INDEX: {index} UL: {UL} LR: {LR}')
            axisfilename = ''.join(['loop_',str(index).zfill(4),'.jpeg'])
            file_to_adorn = os.path.join(context.state['jpeg_save_dir'],axisfilename)
            if os.path.isfile(file_to_adorn):
                draw_bounding_box(file_to_adorn, UL, LR)
            else:
                _logger.debug(f'DID NOT FIND IMAGE: {file_to_adorn}')

            collect_loop_images_update_msg = ' '.join(map(str,['LOOP_INFO', index, status, tipX, tipY, pinBaseX, fiberWidth, loopWidth, boxMinX, boxMaxX, boxMinY, boxMaxY, loopWidthX, isMicroMount, loopClass]))
            context.jpegs.add_results(collect_loop_images_update_msg)
            _logger.info(f'SEND TO DCSS: {collect_loop_images_update_msg}')
            context.get_connection('dcss_conn').send(DcssHtoSOperationUpdate(ao.operation_name,ao.operation_handle,collect_loop_images_update_msg))

    # ==============================================================
    activeOps = context.get_active_operations()
    _logger.debug(f'Active operations post-completed={activeOps}')
    # ==============================================================

@register_message_handler('jpeg_receiver_image_post_request')
def axis_image_request(message:JpegReceiverImagePostRequestMessage, context:DhsContext):
    """
    This handler is triggered when a new jpeg image arrives from the jpeg_receiver. It is then shuttled off to AutoML.

    This may not be fast enough to keep up with 30fps coming from axis?
    """

    _logger.spam(message.file)

    activeOps = context.get_active_operations(operation_name='collectLoopImages')
    if len(activeOps) > 0 and not context.gStopJpegStream:
        activeOp = activeOps[0]
        opName = activeOp.operation_name
        opHandle = activeOp.operation_handle
        # Store a set of images from the most recent collectLoopImages for subsequent analysis with reboxLoopImage
        _logger.debug(f'ADD IMAGE OF: {len(message.file)} BYTES TO JPEG LIST')
        context.jpegs.add_image(message.file)

        # random 12 character string. not using at the moment.
        #image_key = ''.join(choice(ascii_uppercase + digits) for i in range(12))

        if not hasattr(activeOp, 'img_idx'):
            activeOp.img_idx = 1
        image_key = ':'.join([opName,opHandle,str(activeOp.img_idx)])
        save_jpeg(message.file, activeOp.img_idx)
        context.get_connection('automl_conn').send(AutoMLPredictRequest(image_key, message.file))
        activeOp.img_idx += 1
    else:
        _logger.debug(f'RECEVIED JPEG, BUT NOT DOING ANYTHING WITH IT.')

def save_jpeg(image:bytes, index:int=None):
    """
    Save an image to the specified directory, and increment the number.
    e.g. if file_0001.txt exists then teh next file will be file_0002.txt
    """
    newNum = index
    if newNum is None:
        currentImages = glob.glob("JPEGS/*.jpeg")
        numList = [0]
        for img in currentImages:
            i = os.path.splitext(img)[0]
            try:
                num = re.findall('[0-9]+$', i)[0]
                numList.append(int(num))
            except IndexError:
                pass
        numList = sorted(numList)
        newNum = numList[-1]+1

    saveName = 'JPEGS/loop_%04d.jpeg' % newNum

    f = open(saveName, 'w+b')
    f.write(image)
    f.close()

def draw_bounding_box(file_to_adorn:str, upper_left_corner:list, lower_right_corner:list):
    """Use OpenCV to draw a bounding box on a jpeg"""
    image = cv2.imread(file_to_adorn)
    s = tuple(image.shape[1::-1])
    w = s[0]
    h = s[1]

    # represents the top left corner of rectangle in pixels.
    start_point = (math.floor(upper_left_corner[0] * w), math.floor(upper_left_corner[1] * h))
    #_logger.info(f'START: {start_point}')

    # represents the bottom right corner of rectangle in pixels.
    end_point = (math.ceil(lower_right_corner[0] * w), math.ceil(lower_right_corner[1] * h))
    #_logger.info(f'END: {end_point}')

    # Red color in BGR 
    color = (0, 0, 255) 

    # Line thickness of 1 px 
    thickness = 1

    # Using cv2.rectangle() method 
    # Draw a rectangle with red line borders of thickness of 1 px 
    image = cv2.rectangle(image, start_point, end_point, color, thickness)

    outfn = "automl_" + os.path.basename(file_to_adorn)
    outdir = os.path.dirname(file_to_adorn)
    outfile = os.path.join(outdir,"bboxes",outfn)
    _logger.info(f'DREW BOUNDING BOX: {outfile}')

    cv2.imwrite(outfile,image)

def empty_jpeg_dir():
    files = glob.glob('JPEGS/*.jpeg')
    for f in files:
        os.remove(f)
    files = glob.glob('JPEGS/bboxes/*.jpeg')
    for f in files:
        os.remove(f)

dhs = Dhs()
dhs.start()
sigs = {}
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)
