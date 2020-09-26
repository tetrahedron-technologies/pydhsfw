from enum import Enum
from pydhsfw.threads import AbortableThread
from pydhsfw.messages import MessageIn, MessageOut, MessageFactory

class ConnectionMessage(MessageIn):

    def __init__(self, msg):
        super().__init__()
        self._msg = msg

    def get_msg(self):
        return self._msg

class ConnectionConnectedMessage(ConnectionMessage):
    def __init__(self, msg):
        super().__init__(msg)

class ConnectionDisconnectedMessage(ConnectionMessage):
    def __init__(self, msg):
        super().__init__(msg)

class ConnectionShutdownMessage(ConnectionMessage):
    def __init__(self, msg):
        super().__init__(msg)

class MessageReader():
    def read(self):
        pass

class ClientState(Enum):
    DISCONNECTED = 1
    CONNECTING = 2
    CONNECTED = 3
    DISCONNECTING = 4

class Connection:

    def __init__(self, url:str, config:dict={}):
        self._url = url
        self._config = config

    def connect(self):
        pass

    def disconnect(self):
        pass

    def send(self, msg:MessageOut):
        """
        DO docstrings show stuff in MS VSCODE When I hover over the command?
        """
        pass

    def shutdown(self):
        pass

    def wait(self):
        pass

class ConnectionRegistry():

    _registry = {}

    @classmethod
    def _register_connection(cls, connection_scheme:str, connection_cls: Connection):
        cls._registry[connection_scheme]=connection_cls

    @classmethod
    def _get_connection_class(cls, connection_scheme:str):
        return cls._registry.get(connection_scheme, None)        


def register_connection(connection_scheme:str):
    def decorator_register_connection(cls):
        cls._scheme = connection_scheme
        if connection_scheme and issubclass(cls, Connection):
            ConnectionRegistry._register_connection(connection_scheme, cls)
        
        return cls

    return decorator_register_connection

class MessageProcessor():

    def __init__(self):
        pass

    def _queque_message(self, message:MessageIn):
        pass

    def _get_message(self, timeout=None):
        pass

    def _clear_messages(self):
        pass

