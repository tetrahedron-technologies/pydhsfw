from pydhsfw.messages import MessageIn, MessageOut, MessageFactory, MessageReader, MessageProcessor
from pydhsfw.connections import TcpipClientConnection

class DcssMessageIn():
    _split_msg = None

    def __init__(self, split):
        self._split_msg = split
    
    def __str__(self):
        return " ".join(self._split_msg)

    @staticmethod
    def _split(buffer:bytes):
        return buffer.decode('ascii').rstrip('\n\r\x00').split(' ')

    def get_command(self):
        return self._split_msg[0]

    def get_args(self):
        return self._split_msg[1:]

class DcssStoCMessage(MessageIn, DcssMessageIn):
 
    def __init__(self, split):
        DcssMessageIn.__init__(self, split)

    @staticmethod
    def parse(buffer:bytes):
        return DcssStoCMessage(DcssStoCMessage._split(buffer))

    @staticmethod
    def get_command():
        pass

class DcssStoCSendClientType(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @staticmethod
    def get_command():
        return 'stoc_send_client_type'

    @staticmethod
    def parse(buffer:bytes):
        split_msg = DcssStoCMessage._split(buffer)
        if split_msg[0] == DcssStoCSendClientType.get_command():
            return DcssStoCSendClientType(split_msg)
        else:
            return None

class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @staticmethod
    def get_command():
        return 'stoh_register_operation'

    @staticmethod
    def parse(buffer:bytes):
        split_msg = DcssStoCMessage._split(buffer)
        if split_msg[0] == DcssStoHRegisterOperation.get_command():
            return DcssStoHRegisterOperation(split_msg)
        else:
            return None

    def get_operation_name(self):
        return self.get_args()[0]
    
class DcssMessageOut(MessageOut):

    def __init__(self):
        super().__init__()

    def __str__(self):
        return " ".join(self._split_msg)

    _split_msg = None

    def write(self)->bytes:
        buffer = None
        
        if self._split_msg:
            buffer = " ".join(self._split_msg).ljust(200, '\x00').encode('ascii')

        return buffer

    @staticmethod
    def get_command():
        pass

class DcssCtoSMessage(DcssMessageOut):
    def __init__(self):
        super().__init__()


class DcssCtoSClientIsHardware(DcssCtoSMessage):
    
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [DcssCtoSClientIsHardware.get_command(), dhs_name]

    @staticmethod
    def get_command():
        return 'htos_client_is_hardware'


class DcssMessageFactory(MessageFactory):
    def __init__(self):
        super().__init__()

    def readRawMessage(self, reader: MessageReader):
        return reader.read(200)

    def createMessage(self, raw_message:bytes):
        dcss_msg = DcssStoCSendClientType.parse(raw_message)
        if not dcss_msg:
            dcss_msg = DcssStoHRegisterOperation.parse(raw_message)
        return dcss_msg
        
class DcssClientConnection(TcpipClientConnection):
    def __init__(self, msg_processor:MessageProcessor):
        super().__init__(msg_processor, DcssMessageFactory())
