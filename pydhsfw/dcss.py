from pydhsfw.messages import MessageIn, MessageOut, MessageFactory, register_message
from pydhsfw.connection import MessageProcessor, register_connection
from pydhsfw.tcpip import TcpipClientConnection, TcpipSocketReader


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

    @property
    def command(self):
        """
        The command property getter
        """
        return self._split_msg[0]

    @property
    def args(self):
        """
        The args property getter
        """
        return self._split_msg[1:]

class DcssStoCMessage(MessageIn, DcssMessageIn):
 
    def __init__(self, split):
        DcssMessageIn.__init__(self, split)

    def __str__(self):
        return DcssMessageIn.__str__(self)

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

class DcssHtoSMessage(DcssMessageOut):
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

    @property
    def operation_name(self):
        """
        The operation_name getter
        """
        return self.args[0]

    @property
    def operation_hardwareName(self):
        """
        The operation_hardwareName getter
        """
        return self.args[1]

@register_message('stoh_register_real_motor', 'dcss')
class DcssStoHRegisterRealMotor(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def motor_name(self):
        return self.args[0]

    @property
    def motor_hardwareName(self):
        return self.args[1]

@register_message('stoh_register_string', 'dcss')
class DcssStoHRegisterString(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def string_name(self):
        return self.args[0]

    @property
    def string_hardwareName(self):
        return self.args[1]

@register_message('stoh_register_shutter', 'dcss')
class DcssStoHRegisterShutter(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def shutter_name(self):
        return self.args[0]

    @property
    def shutter_status(self):
        return self.args[1]

    @property
    def shutter_hardwareName(self):
        return self.args[2]

@register_message('stoh_register_ion_chamber', 'dcss')
class DcssStoHRegisterIonChamber(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def ion_chamber_name(self):
        return self.args[0]

    @property
    def ion_chamber_hardwareName(self):
        return self.args[1]

    @property
    def ion_chamber_counterChannel(self):
        return self.args[2]

    @property
    def ion_chamber_timer(self):
        return self.args[3]

    @property
    def ion_chamber_timerType(self):
        return self.args[4]

@register_message('stoh_register_pseudo_motor', 'dcss')
class DcssStoHRegisterPseudoMotor(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def pseudo_motor_name(self):
        return self.args[0]

    @property
    def pseudo_motor_hardwareName(self):
        return self.args[1]

@register_message('stoh_register_encoder', 'dcss')
class DcssStoHRegisterEncoder(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def encoder_name(self):
        return self.args[0]

    @property
    def encoder_hardwareName(self):
        return self.args[1]

@register_message('stoh_register_object', 'dcss')
class DcssStoHRegisterObject(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def object_name(self):
        return self.args[0]

    @property
    def object_hardwareName(self):
        return self.args[1]

# stoh_configure_real_motor motoName position upperLimit lowerLimit scaleFactor speed acceleration backlash lowerLimitOn upperLimitOn motorLockOn backlashOn reverseOn
@register_message('stoh_configure_real_motor', 'dcss')
class DcssStoHConfigureRealMotor(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def motor_name(self):
        return self.args[0]

    @property
    def motor_position(self):
        return self.args[1]

    @property
    def motor_upperLimit(self):
        return self.args[2]

    @property
    def motor_lowerLimit(self):
        return self.args[3]

    @property
    def motor_scaleFactor(self):
        return self.args[4]

    @property
    def motor_speed(self):
        return self.args[5]

    @property
    def motor_acceleration(self):
        return self.args[6]

    @property
    def motor_backlash(self):
        return self.args[7]

    @property
    def motor_lowerLimitOn(self):
        return self.args[8]

    @property
    def motor_upperLimitOn(self):
        return self.args[9]

    @property
    def motor_motorLockOn(self):
        return self.args[10]

    @property
    def motor_backlashOn(self):
        return self.args[11]

    @property
    def motor_reverseOn(self):
        return self.args[12]

# stoh_configure_pseudo_motor motorName position upperLimit lowerLimit upperLimitOn lowerLimitOn motorLockOn
@register_message('stoh_configure_pseudo_motor', 'dcss')
class DcssStoHConfigurePseudoMotor(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def motor_name(self):
        return self.args[0]

    @property
    def motor_position(self):
        return self.args[1]

    @property
    def motor_upperLimit(self):
        return self.args[2]

    @property
    def motor_lowerLimit(self):
        return self.args[3]

    @property
    def motor_lowerLimitOn(self):
        return self.args[4]

    @property
    def motor_upperLimitOn(self):
        return self.args[5]

    @property
    def motor_motorLockOn(self):
        return self.args[6]

@register_message('stoh_set_motor_position', 'dcss')
class DcssStoHSetMotorPosition(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def motor_name(self):
        return self.args[0]

    @property
    def motor_position(self):
        return self.args[1]

@register_message('stoh_start_motor_move', 'dcss')
class DcssStoHStartMotorMove(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def motor_name(self):
        return self.args[0]

    @property
    def motor_position(self):
        return self.args[1]

# This command requests that all operations either cease immediately or halt as soon as possible.
#  All motors are stopped and data collection begins shutting down and stopping detector activity.
#  A single argument specifies how motors should be aborted.
#  A value of hard indicates that motors should stop without decelerating.
#  A value of soft indicates that motors should decelerate properly before stopping.
@register_message('stoh_abort_all', 'dcss')
class DcssStoHAbortAll(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def abort_arg(self):
        return self.args[0]

@register_message('stoh_correct_motor_position', 'dcss')
class DcssStoHCorrectMotorPosition(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def motor_name(self):
        return self.args[0]

    @property
    def motor_correction(self):
        return self.args[1]

@register_message('stoh_set_motor_dependency', 'dcss')
class DcssStoHSetMotorDependency(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def motor_name(self):
        return self.args[0]

    @property
    def motor_dependencies(self):
        return self.args[1:]

@register_message('stoh_set_motor_children', 'dcss')
class DcssStoHSetMotorChildren(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def motor_name(self):
        return self.args[0]

    @property
    def motor_children(self):
        return self.args[1:]

@register_message('stoh_set_shutter_state','dcss')
class DcssStoHSetShutterState(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def shutter_name(self):
        return self.args[0]

    @property
    def shutter_state(self):
        return self.args[1]

@register_message('stoh_start_operation','dcss')
class DcssStoHStartOperation(DcssStoCMessage):
    def __init__(self, split):
        super().__init__(split)

    @property
    def operation_name(self):
        return self.args[0]

    @property
    def operation_handle(self):
        return self.args[1]

    @property
    def operation_args(self):
        return self.args[2:]

#Messages Outgoing to DCSS (Hardware TO Server)
@register_message('htos_client_is_hardware')
class DcssHtoSClientIsHardware(DcssHtoSMessage): 
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), dhs_name]

@register_message('htos_motor_move_started')
class DcssHtoSMotorMoveStarted(DcssHtoSMessage): 
    def __init__(self, motor_name:str, new_position:float):
        super().__init__()
        self._split_msg = [self.get_type_id(), motor_name, new_position]

@register_message('htos_motor_move_completed')
class DcssHtoSMotorMoveCompleted(DcssHtoSMessage): 
    def __init__(self, motor_name:str, new_position:float, state:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), motor_name, new_position, state]

@register_message('htos_operation_completed')
class DcssHtoSOperationCompleted(DcssHtoSMessage): 
    def __init__(self, operation_name:str, operation_handle:float, operation_status:str, operation_args:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), operation_name, operation_handle, operation_status, operation_args]

@register_message('htos_operation_update')
class DcssHtoSOperationUpdate(DcssHtoSMessage): 
    def __init__(self, operation_name:str, operation_handle:float, operation_args:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), operation_name, operation_handle, operation_args]

# used?
@register_message('htos_start_operation')
class DcssHtoSStartOperation(DcssHtoSMessage): 
    def __init__(self, operation_info:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), operation_info]

@register_message('htos_update_motor_position')
class DcssHtoSUpdateMotorPosition(DcssHtoSMessage): 
    def __init__(self, motor_name:str, new_position:float, status:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), motor_name, new_position, status]

@register_message('htos_report_ion_chambers')
class DcssHtoSReportIonChamber(DcssHtoSMessage): 
    def __init__(self, ion_chamber_counts:int):
        super().__init__()
        self._split_msg = [self.get_type_id(), ion_chamber_counts]

# device_settings will be an array of values. some being floats and others being ints
# for example (float float float float int int int int int int int int)
# should we break them out here?
# Another complication is that this could be used to configure a STEPPER or a PSEUDO MOTOR
# for STEPPER we need to configure with 12 values
# for PSEUDO we need to configure with 6 values
@register_message('htos_configure_device')
class DcssHtoSConfigureDevice(DcssHtoSMessage): 
    def __init__(self, device_name:str, device_settings:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), device_name, device_settings]

@register_message('htos_send_configuration')
class DcssHtoSSendConfiguration(DcssHtoSMessage): 
    def __init__(self, device_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), device_name]

@register_message('htos_report_shutter_state')
class DcssHtoSReportShutterState(DcssHtoSMessage): 
    def __init__(self, shutter_name:str, state:str, result:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), shutter_name, state, result]


@register_message('htos_limit_hit')
class DcssHtoSLimitHit(DcssHtoSMessage): 
    def __init__(self, state:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), state]

@register_message('htos_simulating_device')
class DcssHtoSSimulatingDevice(DcssHtoSMessage): 
    def __init__(self, state:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), state]

@register_message('htos_motor_correct_started')
class DcssHtoSMotorCorrectStarted(DcssHtoSMessage): 
    def __init__(self, state:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), state]

@register_message('htos_get_encoder_completed')
class DcssHtoSGetEncoderCompleted(DcssHtoSMessage): 
    def __init__(self, encoder_name:str, new_position:float, status:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), encoder_name, new_position, status]

@register_message('htos_set_encoder_completed')
class DcssHtoSSetEncoderCompleted(DcssHtoSMessage): 
    def __init__(self, encoder_name:str, new_position:int, status:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), encoder_name, new_position, status]

@register_message('htos_set_string_completed')
class DcssHtoSSetStringCompleted(DcssHtoSMessage): 
    def __init__(self, string_name:str, status:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), string_name, status]

@register_message('htos_note')
class DcssHtoSNote(DcssHtoSMessage): 
    def __init__(self, nore_message:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), note_message]

@register_message('htos_log')
class DcssHtoSLog(DcssHtoSMessage): 
    def __init__(self, log_message:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), log_message]

@register_message('htos_set_motor_message')
class DcssHtoSSetMotorMessage(DcssHtoSMessage): 
    def __init__(self, motor_name:str):
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
@register_connection('dcss')
class DcssClientConnection(TcpipClientConnection):
    def __init__(self, url:str, config:dict, msg_processor:MessageProcessor):
        super().__init__(url, config, msg_processor, DcssXOS1SocketReader(), DcssMessageFactory())
