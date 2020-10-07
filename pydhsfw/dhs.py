import argparse
import sys
import logging
import signal
from typing import Any
from pydhsfw.messages import IncomingMessageQueue, OutgoingMessageQueue, MessageIn, register_message
from pydhsfw.connection import Connection
from pydhsfw.connectionmanager import ConnectionManager
from pydhsfw.dcss import DcssClientConnection, DcssContext, DcssActiveOperations, DcssOutgoingMessageQueue, DcssMessageQueueDispatcher

_logger = logging.getLogger(__name__)

class DhsContext(DcssContext):
    """
    DhsContext
    """
    def __init__(self, active_operations: DcssActiveOperations, connection_mgr:ConnectionManager, incoming_message_queue:IncomingMessageQueue):
        super().__init__(active_operations)
        self._conn_mgr = connection_mgr
        self._incoming_msg_queue = incoming_message_queue
        self._state = None

    def create_connection(self, connection_name:str, scheme:str, url:str, config:dict={})->Connection:

        outgoing_msg_queue = None
        if scheme == DcssClientConnection._scheme:
            outgoing_msg_queue = DcssOutgoingMessageQueue(self._active_operations)
        else:
            outgoing_msg_queue = OutgoingMessageQueue()

        conn = self._conn_mgr.create_connection(connection_name, scheme, url, self._incoming_msg_queue, outgoing_msg_queue, config)
        if not conn:
            _logger.error(f'Could not create a connection for {scheme}')

        return conn

    def get_connection(self, connection_name:str)->Connection:
        return self._conn_mgr.get_connection(connection_name)

    @property
    def state(self)->Any:
        """
        The state property getter
        """
        return self._state

    @state.setter
    def state(self, state):
        """
        The state property setter
        """
        self._state = state


@register_message('dhs_init')
class DhsInit(MessageIn):
    def __init__(self, parser, args):
        super().__init__()
        self.arg_parser = parser
        self.cmd_args = args

    @property
    def parser(self):
        """
        The parser property getter
        """
        return self.arg_parser

    @property
    def args(self):
        """
        The args property getter
        """
        return self.cmd_args

@register_message('dhs_start')
class DhsStart(MessageIn):
    def __init__(self):
        super().__init__()

class Dhs:
    """
    Main DHS class
    """
    def __init__(self, config:dict={}):
        self._conn_mgr = ConnectionManager()
        self._active_operations = DcssActiveOperations()
        self._incoming_msg_queue = IncomingMessageQueue()
        self._context = DhsContext(self._active_operations, self._conn_mgr, self._incoming_msg_queue)
        self._msg_disp = DcssMessageQueueDispatcher('default', self._incoming_msg_queue, self._context, self._active_operations, config)
        self._init()
        self._conn_mgr.load_registry()

    def _init(self):

        # Add DCSS parsing parameters that all DHSs will need here and pass below, that will give the DHS
        # writers a head start.
        # DHS writer can then handle in Dhs_Init to add Dhs specific parse elements.
        #
        # Send the DHSInit message now out of band of the normal message queue.
        parser = argparse.ArgumentParser(description="DHS Distributed Hardware Server")
        self._msg_disp.process_message_now(DhsInit(parser, sys.argv[1:]))

    def start(self):
        """
        Starts the DHS context and reads in the arg parser
        """
        self._msg_disp.start()
        self._msg_disp.process_message(DhsStart())

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



