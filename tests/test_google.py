from context import pydhsfw
from pydhsfw.messages import MessageFactory, MessageQueue
from pydhsfw.connection import ConnectionBase
from pydhsfw.http import HttpClientTransport, MessageResponseReader, MessageRequestWriter

class GoogleResponseReader(MessageResponseReader):
    pass

class GoogleRequestWriter(MessageRequestWriter):
    pass

class GoogleMessageFactory(MessageFactory):
    pass

class GoogleClientTransport(HttpClientTransport):
    def __init__(self, url:str, config:dict={}):
        super().__init__(url, GoogleResponseReader(), GoogleRequestWriter(), config)

class GoogleClientConnection(ConnectionBase):
    def __init__(self, url:str, incoming_message_queue:MessageQueue, outgoing_message_queue:MessageQueue, config:dict={}):
        super().__init__(url, HttpClientTransport(url, config), incoming_message_queue, outgoing_message_queue, GoogleMessageFactory(), config)
