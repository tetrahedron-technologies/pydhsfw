from urllib.parse import urlparse
from pydhsfw.messages import MessageProcessor, MessageFactory
from pydhsfw.connections import Connection, TcpipClientConnection, MessageReader, TcpipSocketReader
from pydhsfw.dcss import DcssClientConnection

class ConnectionFactory():

    @staticmethod
    def getConnection(url:str, msg_processor:MessageProcessor, msg_reader:MessageReader=None, msg_factory:MessageFactory=None) -> Connection:

        connection = None
        scheme = urlparse(url).scheme
        if scheme == 'tcp':
            reader = TcpipSocketReader
            if not msg_reader or not isinstance(msg_reader, reader):
                raise ValueError(f"Tcp connections require a msg_reader argument that is of type {reader.__class__.__name__}")
            if not msg_factory:
                raise ValueError("Tcp connections require a msg_factory argument")
            connection = TcpipClientConnection(msg_processor, msg_reader, msg_factory)
        elif scheme == 'dcss':
            connection = DcssClientConnection(msg_processor)
            
        return connection