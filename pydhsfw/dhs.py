import argparse
import sys
import logging
import signal
from pydhsfw.messages import MessageIn, MessageQueue, BlockingMessageQueue, register_message
from pydhsfw.connection import Connection
from pydhsfw.processors import Context, MessageQueueDispatcher
from pydhsfw.connectionmanager import ConnectionManager
from pydhsfw.dcss import DcssContext, DcssActiveOperations, DcssOutgoingMessageQueue, DcssMessageQueueDispatcher

_logger = logging.getLogger(__name__)

class DhsContext(DcssContext):
    """
    DhsContext
    """
    def __init__(self, active_operations: DcssActiveOperations, connection_mgr:ConnectionManager, incoming_message_queue:MessageQueue, outgoing_message_queue:MessageQueue):
        super().__init__(active_operations)
        self._conn_mgr = connection_mgr
        self._incoming_msg_queue = incoming_message_queue
        self._outgoing_msg_queue = outgoing_message_queue
        self._state = None

    def create_connection(self, connection_name:str, url:str)->Connection:

        conn = self._conn_mgr.create_connection(connection_name, url, self._incoming_msg_queue, self._outgoing_msg_queue)
        if not conn:
            _logger.error(f'Could not create a connection for {url}')

        return conn

    def get_connection(self, connection_name:str)->Connection:
        return self._conn_mgr.get_connection(connection_name)

    def get_dhs_state(self)->object:
        return self._state

    def set_dhs_state(self, state)->object:
        self._state = state


@register_message('dhs_init')
class DhsInit(MessageIn):
    def __init__(self, parser, args ):
        super().__init__()
        self.arg_parser = parser
        self.cmd_args = args

    def get_parser(self):
        return self.arg_parser

    def get_args(self):
        return self.cmd_args

class Dhs:
    """
    Main DHS class
    """
    def __init__(self, config:dict={}):
        self._conn_mgr = ConnectionManager()
        self._active_operations = DcssActiveOperations()
        self._incoming_msg_queue = BlockingMessageQueue()
        self._outgoing_msg_queue = DcssOutgoingMessageQueue(self._active_operations)
        self._context = DhsContext(self._active_operations, self._conn_mgr, self._incoming_msg_queue, self._outgoing_msg_queue)
        self._msg_disp = DcssMessageQueueDispatcher('default', self._incoming_msg_queue, self._context, self._active_operations, config)

    def start(self):
        """
        Starts the DHS context and reads in the arg parser
        """
        self._msg_disp.start()
        parser = argparse.ArgumentParser(description="DHS Distributed Hardware Server")
        # Add DCSS parsing parameters that all DHSs will need here and pass below, that will give the DHS
        # writers a head start.
        # DHS writer can then handle in Dhs_Init to add Dhs specific parse elements.
        self._incoming_msg_queue._queque_message(DhsInit(parser, sys.argv[1:]))

    def shutdown(self):
        """
        Shuts down the DHS
        """
        self._msg_disp.abort()
        self._conn_mgr.shutdown_connections()

    def wait(self, signal_set:set=None):
        """
        Waits indefinitely for the dhs.shutdown() signal or for an interrupt signal from the OS.

            Parameters:
                signal_set: List of signals as defined in the signals module that will trigger the shutdown.

        """
        if signal_set:
            _sig_map = dict(map(lambda s: (s.value, s.name), signal_set))
            def handler(signal_received, frame):
                # Handle any cleanup here
                sig_e = _sig_map.get(signal_received)
                _logger.info(f'{sig_e} detected. Exiting gracefully')
                self.shutdown()
            for sig in signal_set:
                signal.signal(sig, handler)
    
        self._msg_disp.join()
        self._conn_mgr.wait_connections()



