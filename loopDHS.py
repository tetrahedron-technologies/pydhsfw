import os
import platform
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
import matplotlib
from matplotlib import pyplot as plt
from random import choice
#from string import ascii_uppercase, digits
from dotty_dict.dotty_dict import Dotty
from dotty_dict import dotty as dot

from pydhsfw.processors import  Context, register_message_handler
from pydhsfw.dhs import Dhs, DhsInit, DhsStart, DhsContext
from pydhsfw.dcss import DcssContext, DcssStoCSendClientType, DcssHtoSClientIsHardware, DcssStoHRegisterOperation, DcssStoHStartOperation, DcssHtoSOperationUpdate, DcssHtoSOperationCompleted, register_dcss_start_operation_handler
from pydhsfw.automl import AutoMLPredictRequest, AutoMLPredictResponse
from pydhsfw.jpeg_receiver import JpegReceiverImagePostRequestMessage
from pydhsfw.axis import AxisImageRequestMessage, AxisImageResponseMessage

#_logger = logging.getLogger(__name__)
_logger = verboselogs.VerboseLogger('loopDHS.py')
#_logger.addHandler(logging.StreamHandler())

# Mac OS does not like Tkinter (TkAgg backend) so must use QtAgg
if platform.system() == 'Darwin':
    matplotlib.use('Qt5Agg')

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
        """
        Add the AutoML results to a list for use in reboxLoopImage
        loop_info stored as python list so this is a list of lists.
        """
        self.results.append(result)

    @property
    def number_of_images(self) -> int:
        """Get the number of images in the image list"""
        return self._number_of_images

class LoopDHSConfig(Dotty):
    def __init__(self, conf_dict:dict):
        super().__init__(conf_dict)

    @property
    def dcss_url(self):
        return 'dcss://' + str(self['dcss.host']) + ':' + str(self['dcss.port'])

    @property
    def automl_url(self):
        return 'http://' + str(self['loopdhs.automl.host']) + ':' + str(self['loopdhs.automl.port'])

    @property
    def jpeg_receiver_url(self):
        return 'http://localhost:' + str(self['loopdhs.jpeg_receiver.port'])

    @property
    def axis_url(self):
        return 'http://' + str(self['loopdhs.axis.host']) + ':' + str(self['loopdhs.axis.port'])

    @property
    def axis_camera(self):
        return self['loopdhs.axis.camera']

    @property
    def save_images(self):
        return self['loopdhs.save_image_files']

    @property
    def jpeg_save_dir(self):
        return self['loopdhs.image_save_dir']

class LoopDHSState():
    def __init__(self):
        self._rebox_images = None
        self._collect_images = False

    @property
    def rebox_images(self)->LoopImageSet:
        return self._rebox_images

    @rebox_images.setter
    def rebox_images(self, images:LoopImageSet):
        self._rebox_images = images

    @property
    def collect_images(self)->bool:
        return self._collect_images

    @collect_images.setter
    def collect_images(self, collect:bool):
        self._collect_images = collect

class CollectLoopImageState():
    def __init__(self):
        self._loop_images = LoopImageSet()
        self._image_index = 0

    @property
    def loop_images(self):
        return self._loop_images

    @property
    def image_index(self)->int:
        return self._image_index

    @image_index.setter
    def image_index(self, idx:int):
        self._image_index = idx


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
    _logger.success(f'Initializing DHS')
    loglevel_name = logging.getLevelName(_logger.getEffectiveLevel())
    _logger.success(f'Logging level is set to: {loglevel_name}')
    _logger.success(f'config file: {conf_file}')
    with open(conf_file, 'r') as f:
        yconf = yaml.safe_load(f)
        context.config = LoopDHSConfig(yconf)
    context.config['DHS'] = args.dhs_name
    _logger.success(f'Initializing {context.config["DHS"]}')
    _logger.success(f'DCSS HOST: {context.config["dcss.host"]} PORT: {context.config["dcss.port"]}')
    _logger.success(f'AUTOML HOST: {context.config["loopdhs.automl.host"]} PORT: {context.config["loopdhs.automl.port"]}')
    _logger.success(f'JPEG RECEIVER PORT: {context.config["loopdhs.jpeg_receiver.port"]}')
    _logger.success(f'AXIS HOST: {context.config["loopdhs.axis.host"]} PORT: {context.config["loopdhs.axis.port"]}')
    _logger.success('=============================================')

    context.state = LoopDHSState()

    if context.config.save_images:
        if not os.path.exists(context.config.jpeg_save_dir):
            os.makedirs(''.join([context.config.jpeg_save_dir,'bboxes']))
        else:
            empty_jpeg_dir(context.config.jpeg_save_dir)

@register_message_handler('dhs_start')
def dhs_start(message:DhsStart, context:DhsContext):

    # Connect to DCSS
    context.create_connection('dcss_conn', 'dcss', context.config.dcss_url)
    context.get_connection('dcss_conn').connect()

    # Connect to GCP AutoML docker service
    context.create_connection('automl_conn', 'automl', context.config.automl_url, {'heartbeat_path': '/v1/models/default'})
    context.get_connection('automl_conn').connect()

    # Connect to an AXIS Video Server
    context.create_connection('axis_conn', 'axis', context.config.axis_url)
    context.get_connection('axis_conn').connect()

    # Open a jpeg receiving port. Not sure this needs to be open all the time.
    # but Giles suggests that this is safe because unexpected data arriving on jpeg_receiver_url
    # will be ignored if there is no activeOp to deal with it.
    context.create_connection('jpeg_receiver_conn', 'jpeg_receiver', context.config.jpeg_receiver_url)
    #context.get_connection('jpeg_receiver_conn').connect()

@register_message_handler('stoc_send_client_type')
def dcss_send_client_type(message:DcssStoCSendClientType, context:Context):
    context.get_connection('dcss_conn').send(DcssHtoSClientIsHardware(context.config['DHS']))

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

    # 1. Instantiate CollectLoopImageState.
    context.get_active_operations(operation_name=message.operation_name, operation_handle=message.operation_handle)[0].state = CollectLoopImageState()

    # 2. Set image collection flag.
    context.state.collect_images = True
    
    # 3. Open the JPRG receiver port.
    context.get_connection('jpeg_receiver_conn').connect()

    # 4. Send an operation update message to DCSS to trigger both sample rotation and axis server to send images.
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
    # 1. Request single jpeg image from axis video server. takes camera as arg.
    cam = str(context.config.axis_camera)
    context.get_connection('axis_conn').send(AxisImageRequestMessage(''.join(['camera=',cam])))
 

@register_dcss_start_operation_handler('getLoopInfo')
def get_loop_info(message:DcssStoHStartOperation, context:DcssContext):
    """
    This operation should return full suite of info about a single image.

    DCSS may send a single arg pinBaseSizeHint, but I think we can ignore it.
    """
    _logger.info(f'FROM DCSS: {message}')
    # 1. Request single jpeg image from axis video server. takes camera as arg.
    cam = str(context.config.axis_camera)
    context.get_connection('axis_conn').send(AxisImageRequestMessage(''.join(['camera=',cam])))
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

    # 1. Set image collection to False.
    context.state.collect_images = False

    # 2. Shutdown JPEG receiver port.
    context.get_connection('jpeg_receiver_conn').disconnect()

    # 3. Send operation completed message to DCSS
    context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(message.operation_name,message.operation_handle,'normal','flag set'))

    # 4. If there is an active collectLoopImages operation send a completed message.
    activeOps = context.get_active_operations()
    for ao in activeOps:
        if ao.operation_name == 'collectLoopImages':
            if context.config.save_images:
                write_results(context.config.jpeg_save_dir, ao.state.loop_images)
                plot_results(context.config.jpeg_save_dir, ao.state.loop_images)
            context.state.rebox_images = ao.state.loop_images
            context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(ao.operation_name,ao.operation_handle,'normal','done'))

def write_results(jpeg_dir:str, images:LoopImageSet):
    """Writes out current contents of the jpeg list"""
    timestr = time.strftime("%Y%m%d-%H%M%S")
    fn = ''.join(['results_',timestr,'.txt'])
    results_file = os.path.join(jpeg_dir,fn)
    with open(results_file, 'w') as f:
        for item in images.results:
            f.write('%s\n' % item)

def plot_results(jpeg_dir:str, images:LoopImageSet):
    """Makes a simple plot of image index vs loopWidth"""
    timestr = time.strftime("%Y%m%d-%H%M%S")
    i = [e[1] for e in images.results]
    _logger.spam(f'PLOT INDICES: {i}')
    loopWidths = [e[7] for e in images.results]
    _logger.spam(f'PLOT LOOP WIDTHS: {loopWidths}')
    plt.plot(i,loopWidths)
    plt.xlabel('image index')
    plt.ylabel('loop width')
    plt.title(' '.join(['loopWidth',timestr]))
    fn = ''.join(['plot_loop_widths_',timestr,'.png'])
    results_plot = os.path.join(jpeg_dir,fn)
    plt.savefig(results_plot)

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
    (resultMaxY - resultMinY) <--- loopWidth

    """
    _logger.info(f'FROM DCSS: {message}')
    rebox_image = int(message.operation_args[0])
    previous_results = context.state.rebox_images.results[rebox_image]
    _logger.info(f'REQUEST REBOX OF IMAGE: {rebox_image} RESULTS: {previous_results}')
    index = previous_results[1]
    loopWidth = previous_results[7]
    boxMinY = previous_results[10]
    boxMaxY = previous_results[11]
    results = [index, boxMinY, boxMaxY, loopWidth]
    # transmorgrify into space-seperated list for Tcl.
    return_msg = ' '.join(map(str,results))
    context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(message.operation_name,message.operation_handle,'normal', return_msg))

@register_message_handler('automl_predict_response')
def automl_predict_response(message:AutoMLPredictResponse, context:DcssContext):
    """
    This handler will process inference results from AutoML.
    """

    # ==============================================================
    activeOps = context.get_active_operations()
    _logger.debug(f'Active operations pre-completed={activeOps}')
    # ==============================================================

    # so we can do some sort of results filtering here I suppose.
    if message.get_score(0) < 0.50:
        _logger.warning(f'TOP AUTOML SCORE IS BELOW 0.50 THRESHOLD: {message.get_score(0)}')
        status = 'failed'
        result = ['no loop detected']
    else:
        status = 'normal'
        result = []

    # for i in range(5):
    #     score = message.get_score(i)
    #     _logger.debug(f'INFERENCE RESULT #{i} HAS SCORE: {score}')

    # do the maths on AutoML response values
    tipX = round(message.bb_maxX, 5)
    # this is not ideal, but for now the best I can come up with is by adding minY to 1/2 the loopWidth
    tipY = round(message.bb_minY + ((message.bb_maxY - message.bb_minY)/2), 5)
    pinBaseX = 0.111 # will add once we have AutoML model that recognizes pins
    fiberWidth = 0.222 # not sure we can or need to support this.
    loopWidth = round((message.bb_maxY - message.bb_minY), 5)
    boxMinX = round(message.bb_minX, 5)
    boxMaxX = round(message.bb_maxX, 5)
    boxMinY = round(message.bb_minY, 5)
    boxMaxY = round(message.bb_maxY, 5)
    # need to double check that this is correct, and what it is used for.
    loopWidthX = round((message.bb_maxX - message.bb_minX), 5)
    if message.top_classification == 'mitegen':
        isMicroMount = 1
    else:
        isMicroMount = 0
    loopClass = message.top_classification
    loopScore = round(message.top_score, 5)

    for ao in activeOps:
        if ao.operation_name == 'predictOne':
            result = [message.image_key, message.top_score, message.top_bb, message.top_classification, message.top_score]
            msg = ' '.join(map(str,result))
            _logger.info(f'SEND TO DCSS: {msg}')
            context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(ao.operation_name, ao.operation_handle, status, msg))
        elif ao.operation_name == 'getLoopTip':
            result = [tipX, tipY]
            msg = ' '.join(map(str,result))
            _logger.info(f'SEND TO DCSS: {msg}')
            context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(ao.operation_name, ao.operation_handle, status, msg))
        elif ao.operation_name == 'getLoopInfo':
            result = [tipX, tipY, pinBaseX, fiberWidth, loopWidth, boxMinX, boxMaxX, boxMinY, boxMaxY, loopWidthX, isMicroMount]
            msg = ' '.join(map(str,result))
            _logger.info(f'SEND TO DCSS: {msg}')
            context.get_connection('dcss_conn').send(DcssHtoSOperationCompleted(ao.operation_name, ao.operation_handle, status, msg))
        elif ao.operation_name == 'collectLoopImages' and context.state.collect_images:
            index = message.image_key.split(':')[2]
            result = ['LOOP_INFO', index, status, tipX, tipY, pinBaseX, fiberWidth, loopWidth, boxMinX, boxMaxX, boxMinY, boxMaxY, loopWidthX, isMicroMount, loopClass, loopScore]
            msg = ' '.join(map(str,result))
            ao.state.loop_images.add_results(result)

            # Draw the AutoML bounding box
            if context.config.save_images:
                upper_left = [message.bb_minX,message.bb_minY]
                lower_right = [message.bb_maxX,message.bb_maxY]
                tip = [tipX, tipY]
                _logger.info(f'DRAW BOUNDING BOX FOR IMAGE: {index} UL: {upper_left} LR: {lower_right} TIP: {tip}')
                axisfilename = ''.join(['loop_',str(index).zfill(4),'.jpeg'])
                file_to_adorn = os.path.join(context.config.jpeg_save_dir, axisfilename)
                if os.path.isfile(file_to_adorn):
                    draw_bounding_box(file_to_adorn, upper_left, lower_right, tip)
                else:
                    _logger.warning(f'DID NOT FIND IMAGE: {file_to_adorn}')
            
            _logger.info(f'SEND UPDATE TO DCSS: {msg}')
            context.get_connection('dcss_conn').send(DcssHtoSOperationUpdate(ao.operation_name, ao.operation_handle, msg))

    # ==============================================================
    activeOps = context.get_active_operations()
    _logger.debug(f'Active operations post-completed={activeOps}')
    # ==============================================================

@register_message_handler('jpeg_receiver_image_post_request')
def jpeg_receiver_image_post_request(message:JpegReceiverImagePostRequestMessage, context:DhsContext):
    """
    This handler is triggered when a new JPEG image arrives on the jpeg receiver port.
    It is then shuttled off to AutoML.
    """

    _logger.spam(message.file)

    activeOps = context.get_active_operations(operation_name='collectLoopImages')
    if len(activeOps) > 0 and context.state.collect_images:
        activeOp = activeOps[0]
        opName = activeOp.operation_name
        opHandle = activeOp.operation_handle
        # Store a set of images from the most recent collectLoopImages for subsequent analysis with reboxLoopImage
        _logger.debug(f'ADD {len(message.file)} BYTE IMAGE TO JPEG LIST')
        activeOp.state.loop_images.add_image(message.file)

        image_key = ':'.join([opName,opHandle,str(activeOp.state.image_index)])
        if context.config.save_images:
            save_jpeg(message.file, activeOp.state.image_index)
        context.get_connection('automl_conn').send(AutoMLPredictRequest(image_key, message.file))
        activeOp.state.image_index += 1
    else:
        _logger.warning(f'RECEVIED JPEG, BUT NOT DOING ANYTHING WITH IT.')

@register_message_handler('axis_image_response')
def axis_image_response(message:AxisImageResponseMessage, context:DhsContext):
    """
    This message handler will be used for both getLoopTip and getLoopInfo operations
    It will process a single JPEG image received from an AXIS video server.
    """
    _logger.debug(f'RECEIVED {message.file_length} BYTE IMAGE FROM AXIS VIDEO SERVER.')
    activeOps = context.get_active_operations()
    for ao in activeOps:
        if ao.operation_name == 'getLoopTip' or 'getLoopInfo':
            #activeOps = context.get_active_operations(operation_name='getLoopTip')
            #if len(activeOps) > 0:
            #activeOp = activeOps[0]
            opName = ao.operation_name
            opHandle = ao.operation_handle
            image_key = ':'.join([opName,opHandle])
            context.get_connection('automl_conn').send(AutoMLPredictRequest(image_key, message.file))
        else:
            _logger.warning(f'RECEVIED JPEG, BUT NOT DOING ANYTHING WITH IT.')

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
    _logger.info(f'SAVED JPEG IMAGE FILE: {saveName}')

def draw_bounding_box(file_to_adorn:str, upper_left_corner:list, lower_right_corner:list, tip:list):
    """Use OpenCV to draw a bounding box on a jpeg"""
    image = cv2.imread(file_to_adorn)
    s = tuple(image.shape[1::-1])
    w = s[0]
    h = s[1]
    tipX_frac = tip[0]
    tipY_frac = tip[1]
    tipX = round(tipX_frac * w)
    tipY = round(tipY_frac * h)
    crosshair_size = round(0.1 * h)
    # represents the top left corner of rectangle in pixels.
    start_point = (math.floor(upper_left_corner[0] * w), math.floor(upper_left_corner[1] * h))
    #_logger.info(f'START: {start_point}')

    # represents the bottom right corner of rectangle in pixels.
    end_point = (math.ceil(lower_right_corner[0] * w), math.ceil(lower_right_corner[1] * h))
    #_logger.info(f'END: {end_point}')

    # Red color in BGR 
    color = (0, 0, 255)
    xhair_color = (0, 255, 0)

    # Line thickness of 1 px 
    thickness = 1

    image = cv2.rectangle(image, start_point, end_point, color, thickness)
    cross_hair_horz = [(tipX - crosshair_size, tipY),(tipX + crosshair_size, tipY)]
    cross_hair_vert = [(tipX, tipY - crosshair_size),(tipX, tipY + crosshair_size)]
    image = cv2.line(image,cross_hair_horz[0],cross_hair_horz[1],xhair_color,thickness)
    image = cv2.line(image,cross_hair_vert[0],cross_hair_vert[1],xhair_color,thickness)

    outfn = "automl_" + os.path.basename(file_to_adorn)
    outdir = os.path.dirname(file_to_adorn)
    outfile = os.path.join(outdir,"bboxes",outfn)
    _logger.info(f'DREW BOUNDING BOX: {outfile}')

    cv2.imwrite(outfile,image)

def empty_jpeg_dir(directory:str):

    files = glob.glob(''.join([directory,'*.jpeg']))
    for f in files:
        os.remove(f)
    files = glob.glob(''.join([directory,'bboxes/*.jpeg']))
    for f in files:
        os.remove(f)

dhs = Dhs()
dhs.start()
sigs = {}
if __name__ == '__main__':
    sigs = {signal.SIGINT, signal.SIGTERM}
dhs.wait(sigs)
