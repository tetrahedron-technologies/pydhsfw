from pydhsfw.messages import MessageIn, MessageOut, MessageFactory, MessageProcessor, register_message
from pydhsfw.connections import TcpipClientConnection, TcpipSocketReader


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



#Messages Incoming from DCSS
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

    def get_operation_hardwareName(self):
        return self.get_args()[1]

@register_message('stoh_register_real_motor', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_hardwareName(self):
        return self.get_args()[1]

@register_message('stoh_register_string', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_string_name(self):
        return self.get_args()[0]

    def get_string_hardwareName(self):
        return self.get_args()[1]

@register_message('stoh_register_shutter', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_shutter_name(self):
        return self.get_args()[0]

    def get_shutter_status(self):
        return self.get_args()[1]

    def get_shutter_hardwareName(self):
        return self.get_args()[2]

@register_message('stoh_register_ion_chamber', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_ion_chamber_name(self):
        return self.get_args()[0]

    def get_ion_chamber_hardwareName(self):
        return self.get_args()[1]

    def get_ion_chamber_counterChannel(self):
        return self.get_args()[2]

    def get_ion_chamber_timer(self):
        return self.get_args()[3]

    def get_ion_chamber_timerType(self):
        return self.get_args()[4]

@register_message('stoh_register_pseudo_motor', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_pseudo_motor_name(self):
        return self.get_args()[0]

    def get_pseudo_motor_hardwareName(self):
        return self.get_args()[1]

@register_message('stoh_register_encoder', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_encoder_name(self):
        return self.get_args()[0]

    def get_encoder_hardwareName(self):
        return self.get_args()[1]

@register_message('stoh_register_object', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_object_name(self):
        return self.get_args()[0]

    def get_object_hardwareName(self):
        return self.get_args()[1]


@register_message('stoh_set_motor_position', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_position(self):
        return self.get_args()[1]

@register_message('stoh_start_motor_move', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_position(self):
        return self.get_args()[1]

@register_message('stoh_abort_all', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_abort_args(self):
        return self.get_args()[0]

@register_message('stoh_correct_motor_position', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_correction(self):
        return self.get_args()[1]

@register_message('stoh_set_motor_dependency', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_dependencies(self):
        return self.get_args()[0]

@register_message('stoh_set_motor_children', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_children(self):
        return self.get_args()[1]

#Messages Outgoing to DCSS (Hardware TO Server)
@register_message('htos_client_is_hardware')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), dhs_name]

@register_message('htos_motor_move_started')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), motor_name, new_position]

@register_message('htos_motor_move_completed')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), motor_name, new_position, state]

@register_message('htos_operation_completed')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), operation_info]

@register_message('htos_operation_update')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), operation_info]

@register_message('htos_start_operation')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), operation_info]

@register_message('htos_update_motor_position')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), motor_name, new_position, status]

@register_message('htos_report_ion_chambers')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), ion_chamber_counts]

@register_message('htos_configure_device')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), device_name]

@register_message('htos_send_configuration')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), device_name]

@register_message('htos_report_shutter_state')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), shutter_name, state, result]


@register_message('htos_limit_hit')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), state]

@register_message('htos_simulating_device')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), state]

@register_message('htos_motor_correct_started')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), state]

@register_message('htos_get_encoder_completed')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), encoder_name, new_position, status]

@register_message('htos_set_encoder_completed')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), encoder_name, new_position, status]

@register_message('htos_set_string_completed')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), string_name, status]

@register_message('htos_note')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), note_message]

@register_message('htos_log')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), log_message]

@register_message('htos_set_motor_message')
class DcssCtoSClientIsHardware(DcssCtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), motor_name,]

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
class DcssClientConnection(TcpipClientConnection):
    def __init__(self, msg_processor:MessageProcessor):
        super().__init__(msg_processor, DcssXOS1SocketReader(), DcssMessageFactory())
