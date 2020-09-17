import sys
import logging
import signal
from pydhsfw.messages import MessageIn, register_message
from pydhsfw.connection import Connection
from pydhsfw.processors import Context, MessageDispatcher
from pydhsfw.connectionmanager import ConnectionManager

_logger = logging.getLogger(__name__)

class DhsContext(Context):

    def __init__(self, connection_mgr):
        self._conn_mgr = connection_mgr
        self._msg_processor = MessageDispatcher('default', self)
        self._state = None

    def create_connection(self, connection_name:str, url:str)->Connection:

        conn = self._conn_mgr.create_connection(connection_name, url, self._msg_processor)
        conn.start()
        return conn

    def get_connection(self, connection_name:str)->Connection:
        return self._conn_mgr.get_connection(connection_name)

    def get_dhs_state(self)->object:
        return self._state

    def set_dhs_state(self, state)->object:
        self._state = state

    def _get_msg_disp(self)->MessageDispatcher:
        return self._msg_processor


@register_message('dhs_init')
class DhsInit(MessageIn):
    def __init__(self):
        super().__init__()
        self.initialize_logger()

    def setup_logging(self, loglevel):
        """Setup basic logging

        Args:
        loglevel (int): minimum loglevel for emitting messages
        """
        logformat = "[%(asctime)s] %(levelname)s:%(name)s:%(message)s"
        logging.basicConfig(level=loglevel, stream=sys.stdout, format=logformat, datefmt="%Y-%m-%d %H:%M:%S")

    def initialize_logger(self):
        # once this is merged with the argparse branch we can pass in loglevel on the command line.
        # harcoded as INFO for now.
        self.setup_logging(logging.INFO)
    
    


class Dhs:
    def __init__(self):
        self._context = DhsContext(ConnectionManager())

    def start(self):
        self._context._get_msg_disp().start()
        self._context._get_msg_disp()._queque_message(DhsInit())

    def shutdown(self):
        self._context._get_msg_disp().abort()
        self._context._conn_mgr.shutdown_connections()

    def wait(self, signal_set:set=None):
        '''
        Waits indefinitely for the dhs.shutdown() signal or for an interrupt signal from the OS.

            Parameters:
                    signal_set: List of signals as defined in the signals module that will trigger the shudown.
    
        '''
        if signal_set:
            _sig_map = dict(map(lambda s: (s.value, s.name), signal_set))
            def handler(signal_received, frame):
                # Handle any cleanup here
                sig_e = _sig_map.get(signal_received)
                print(f'{sig_e} detected. Exiting gracefully')
                self.shutdown()
            for sig in signal_set:
                signal.signal(sig, handler)
    
        self._context._get_msg_disp().join()
        self._context._conn_mgr.wait_connections()



