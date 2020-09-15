from urllib.parse import urlparse
from pydhsfw.messages import MessageProcessor, MessageFactory
from pydhsfw.connections import Connection


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

class ConnectionManager:
    def __init__(self):
        self._connections = {}

    def create_connection(self, name:str, url:str, msg_processor:MessageProcessor, config:dict=None)->Connection:
        
        conn = None
        
        if name in self._connections.keys():
            raise ValueError('Connection name already exists')

        conn = ConnectionFactory.create_connection(url, msg_processor, config)
        if conn:
            self._connections[name] = conn

        return conn

    def get_connection(self, name:str)->Connection:
        return self._connections.get(name, None)

    def start_connections(self):
        for conn in self._connections.values():
            conn.start()

    def stop_connections(self):
        for conn in self._connections.values():
            conn.exit()

class ConnectionFactory():

    @staticmethod
    def create_connection(url:str, msg_processor:MessageProcessor, config:dict=None) -> Connection:

        connection = None
        scheme = urlparse(url).scheme
        conn_cls = ConnectionRegistry._get_connection_class(scheme)
        if conn_cls:
            connection = conn_cls(url, config, msg_processor)
            
        return connection