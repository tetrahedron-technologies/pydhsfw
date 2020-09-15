from pydhsfw.messages import MessageIn, MessageOut, MessageFactory, MessageProcessor, register_message
from pydhsfw.connections import TcpipClientConnection, TcpipSocketReader
from pydhsfw.connectionmanager import register_connection


#Dcss Message Base Classes
class DcssMessageIn():

    def __init__(self, split):
        self._split_msg = split
    
    def __str__(self):
        return " ".join(self._split_msg)

    @staticmethod
    def _split(buffer:bytes):
        return buffer.decode('ascii').rstrip('\n\r\x00').split(' ')

    @staticmethod
    def parse_type_id(buffer:bytes):
        return DcssMessageIn._split(buffer)[0]

    def get_command(self):
        return self._split_msg[0]

    def get_args(self):
        return self._split_msg[1:]

class DcssStoCMessage(MessageIn, DcssMessageIn):
 
    def __init__(self, split):
        DcssMessageIn.__init__(self, split)

    @classmethod
    def parse(cls, buffer:bytes):
        
        msg = None
        
        split = DcssMessageIn._split(buffer)
        if split[0] == cls.get_type_id():
            msg = cls(split)
        
        return msg

class DcssMessageOut(MessageOut):

    def __init__(self):
        super().__init__()
        self._split_msg = None

    def __str__(self):
        return " ".join(self._split_msg)

    def write(self)->bytes:
        buffer = None
        
        if self._split_msg:
            buffer = " ".join(self._split_msg).ljust(200, '\x00').encode('ascii')

        return buffer

class DcssCtoSMessage(DcssMessageOut):
    def __init__(self):
        super().__init__()



#Dcss Incoming Messages
@register_message('stoc_send_client_type', 'dcss')
class DcssStoCSendClientType(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

@register_message('stoh_register_operation', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_operation_name(self):
        return self.get_args()[0]
    


#Dcss Outgoing Messages
@register_message('htos_client_is_hardware')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), dhs_name]



#Message Factory
class DcssMessageFactory(MessageFactory):
    def __init__(self):
        super().__init__('dcss')

    def _parse_type_id(self, raw_msg:bytes):
        return DcssMessageIn.parse_type_id(raw_msg)



#Dcss xos v1 Reader
class DcssXOS1SocketReader(TcpipSocketReader):
    def read_socket(self, sock):
        return self._read(sock, msglen=200)


#DcssClientConnection
@register_connection('dcss')
class DcssClientConnection(TcpipClientConnection):
    def __init__(self, url:str, config:dict, msg_processor:MessageProcessor):
        super().__init__(url, config, msg_processor, DcssXOS1SocketReader(), DcssMessageFactory())
