from urllib.parse import urlparse
from pydhsfw.messages import MessageFactory, MessageQueue
from pydhsfw.connection import Connection, ConnectionRegistry
from pydhsfw.processors import Context

class ConnectionManager:
    def __init__(self):
        self._connections = {}

    def create_connection(self, name:str, url:str, incoming_message_queue:MessageQueue, outgoing_message_queue:MessageQueue, config:dict={})->Connection:
        
        conn = None
        
        if name in self._connections.keys():
            raise ValueError('Connection name already exists')

        conn = ConnectionFactory.create_connection(url, incoming_message_queue, outgoing_message_queue, config)
        if conn:
            self._connections[name] = conn

        return conn

    def get_connection(self, name:str)->Connection:
        return self._connections.get(name, None)

    def start_connections(self):
        for conn in self._connections.values():
            conn.start()

    def shutdown_connections(self):
        for conn in self._connections.values():
            conn.shutdown()

    def wait_connections(self):
        for conn in self._connections.values():
            conn.wait()

class ConnectionFactory():

    @staticmethod
    def create_connection(url:str, incoming_message_queue:MessageQueue, outgoing_message_queue:MessageQueue, config:dict=None) -> Connection:

        connection = None
        scheme = urlparse(url).scheme
        conn_cls = ConnectionRegistry._get_connection_class(scheme)
        if conn_cls:
            connection = conn_cls(url, incoming_message_queue, outgoing_message_queue, config)
            
        return connection