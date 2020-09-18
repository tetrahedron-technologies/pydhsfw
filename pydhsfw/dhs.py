import argparse
import sys
import logging
import signal
from pydhsfw.messages import MessageIn, register_message
from pydhsfw.connection import Connection
from pydhsfw.processors import Context, MessageDispatcher
from pydhsfw.connectionmanager import ConnectionManager

_logger = logging.getLogger(__name__)

class DhsContext(Context):
    '''DhsContext
    '''
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
    def __init__(self, parser, args, conf_file ):
        super().__init__()
        self.arg_parser = parser
        self.cmd_args = args
        self.conf_file = conf_file

    def get_parser(self):
        return self.arg_parser

    def get_args(self):
        return self.cmd_args

    def get_conf_file(self):
        return self.conf_file

class Dhs:
    '''Main DHS class
    '''
    def __init__(self):
        self._context = DhsContext(ConnectionManager())

    def start(self):
        self._context._get_msg_disp().start()
        parser = argparse.ArgumentParser(description="DHS Distributed Hardware Server")
        # Add DCSS parsing parameters that all DHSs will need here and pass below, that will give the DHS
        # writers a head start.
        # DHS writer can then handle in Dhs_Init to add Dhs specific parse elements.
        #
        # hard code a config file? seems non-optimal. Would be good if we could parse beamline name from
        # the command line and then use that to decide which config file to use, but it seems we're too
        # early to use argparse....
        config = 'config/bl831.config'
        self._context._get_msg_disp()._queque_message(DhsInit(parser, sys.argv[1:], config))

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
                _logger.info(f'{sig_e} detected. Exiting gracefully')
                self.shutdown()
            for sig in signal_set:
                signal.signal(sig, handler)
    
        self._context._get_msg_disp().join()
        self._context._conn_mgr.wait_connections()



