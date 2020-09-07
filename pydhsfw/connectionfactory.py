from urllib.parse import urlparse
from pydhsfw.messages import MessageProcessor, MessageFactory
from pydhsfw.connections import Connection, TcpipClientConnection
from pydhsfw.dcss import DcssClientConnection

class ConnectionFactory():

    @staticmethod
    def getConnection(url:str, msg_processor:MessageProcessor, msg_factory:MessageFactory=None) -> Connection:

        connection = None
        scheme = urlparse(url).scheme
        if scheme == 'tcp':
            if not msg_factory:
                raise ValueError("Tcp connections require a msg_factory argument")
            connection = TcpipClientConnection(msg_processor, msg_factory)
        elif scheme == 'dcss':
            connection = DcssClientConnection(msg_processor)
            
        return connection