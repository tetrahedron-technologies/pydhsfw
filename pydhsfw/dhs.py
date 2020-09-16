from pydhsfw.messages import MessageIn, register_message
from pydhsfw.connection import Connection
from pydhsfw.processors import Context, MessageDispatcher
from pydhsfw.connectionmanager import ConnectionManager

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

class Dhs:
    def __init__(self):
        self._context = DhsContext(ConnectionManager())

    def start(self):
        self._context._get_msg_disp().start()
        self._context._get_msg_disp()._queque_message(DhsInit())
        return self

    def shutdown(self):
        self._context._get_msg_disp().abort()
        self._context._conn_mgr.shutdown_connections()
        return self

    def wait(self):
        self._context._get_msg_disp().join()
        self._context._conn_mgr.wait_connections()
        return self



