from pydhsfw.messages import MessageIn, MessageOut, MessageFactory, register_message
from pydhsfw.connection import MessageProcessor, register_connection
from pydhsfw.tcpip import TcpipClientConnection, TcpipSocketReader

# --------------------------------------------------------------------------
# DCSS Message Base Classes
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

# --------------------------------------------------------------------------
# Messages Incoming from DCSS
@register_message('stoc_send_client_type', 'dcss')
class DcssStoCSendClientType(DcssStoCMessage):
    """The initial command received from DCSS.

    The new DHS opens a socket connection to the hardware listening port on DCSS.
    DHS begins reading and handling messages sent from DCSS.
    DCSS will send a stoc_send_client_type to the DHS.
    The DHS has 1 second to respond with the following message:
    htos_client_is_hardware dhsName

    At this point DCSS will disconnect a DHS for the following reasons:

    The DHS does not respond within 1 second.
    The DHS does not respond with the name of a DHS listed within the database.dat file.
    There is already a DHS connected with the same name as the DHS that is currently trying to connect.

    """
    def __init__(self, split):
        super().__init__(split)

@register_message('stoh_register_operation', 'dcss')
class DcssStoHRegisterOperation(DcssStoCMessage):
    """Server To Hardware Register Operation

    This message is received from DCSS to tell the DHS which operations it is responsible for.

    This message will be accompanied by two fields:
    1. the name of the operation as it is known in the DCSS database
    2. the name of the operation as it is known in the DHS code

    note: these are almost always identical
    """
    def __init__(self, split):
        super().__init__(split)

    def get_operation_name(self):
        return self.get_args()[0]

    def get_operation_hardwareName(self):
        return self.get_args()[1]

@register_message('stoh_register_real_motor', 'dcss')
class DcssStoHRegisterRealMotor(DcssStoCMessage):
    """Server To Hardware Register Real Motor

    This message is received from DCSS to tell the DHS which real motor(s) it is responsible for.

    This message will be accompanied by two fields:
    1. the name of the motor as it is known in the DCSS database
    2. the name of the motor as it is known in the DHS code

    note: these are almost always identical
    """
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_hardwareName(self):
        return self.get_args()[1]

@register_message('stoh_register_pseudo_motor', 'dcss')
class DcssStoHRegisterPseudoMotor(DcssStoCMessage):
    """Server To Hardware Register Pseudo Motor

    This message is received from DCSS to tell the DHS which pseudo motor(s) it is responsible for.

    This message will be accompanied by two fields:
    1. the name of the motor as it is known in the DCSS database
    2. the name of the motor as it is known in the DHS code

    note: these are almost always identical
    """
    def __init__(self, split):
        super().__init__(split)

    def get_pseudo_motor_name(self):
        return self.get_args()[0]

    def get_pseudo_motor_hardwareName(self):
        return self.get_args()[1]

@register_message('stoh_register_string', 'dcss')
class DcssStoHRegisterString(DcssStoCMessage):
    """Server To Hardware Register String

    This message is received from DCSS to tell the DHS which string(s) it is responsible for.

    This message will be accompanied by two fields:
    1. the name of the string as it is known in the DCSS database
    2. the name of the string as it is known in the DHS code

    note: these are almost always identical
    """
    def __init__(self, split):
        super().__init__(split)

    def get_string_name(self):
        return self.get_args()[0]

    def get_string_hardwareName(self):
        return self.get_args()[1]

@register_message('stoh_register_shutter', 'dcss')
class DcssStoHRegisterShutter(DcssStoCMessage):
    """Server To Hardware Register Shutter

    This message is received from DCSS to tell the DHS which shutters(s) it is responsible for.

    This message will be accompanied by three fields:
    1. the name of the shutter as it is known in the DCSS database
    2. the state of the shutter (desired state or actual state?)
    3. the name of the shutter as it is known in the DHS code

    note: fields 1 and 3 are almost always identical
    """
    def __init__(self, split):
        super().__init__(split)

    def get_shutter_name(self):
        return self.get_args()[0]

    def get_shutter_status(self):
        return self.get_args()[1]

    def get_shutter_hardwareName(self):
        return self.get_args()[2]

@register_message('stoh_register_ion_chamber', 'dcss')
class DcssStoHRegisterIonChamber(DcssStoCMessage):
    """Server To Hardware Register Ion Chamber

    This message is received from DCSS to tell the DHS which ion chamber(s) it is responsible for.

    This message will be accompanied by five fields:
    1. the name of the ion chamber as it is known in the DCSS database.
    2. the name of the ion chamber as it is known in the DHS code.
    3. ion chamber counter channel.
    4. ion chamber timer.
    5. ion chamber timer type.

    note: fields 1 and 2 almost always identical
    """
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

@register_message('stoh_register_encoder', 'dcss')
class DcssStoHRegisterEncoder(DcssStoCMessage):
    """Server To Hardware Register Encoder

    This message is received from DCSS to tell the DHS which encoder(s) it is responsible for.

    This message will be accompanied by two fields:
    1. the name of the encoder as it is known in the DCSS database
    2. the name of the encoder as it is known in the DHS code

    note: these are almost always identical
    """
    def __init__(self, split):
        super().__init__(split)

    def get_encoder_name(self):
        return self.get_args()[0]

    def get_encoder_hardwareName(self):
        return self.get_args()[1]

@register_message('stoh_register_object', 'dcss')
class DcssStoHRegisterObject(DcssStoCMessage):
    """Server to Hardware Register Object

    Not sure if this is used or what it is used for.
    """
    def __init__(self, split):
        super().__init__(split)

    def get_object_name(self):
        return self.get_args()[0]

    def get_object_hardwareName(self):
        return self.get_args()[1]

@register_message('stoh_configure_real_motor', 'dcss')
class DcssStoHConfigureRealMotor(DcssStoCMessage):
    """Server To Hardware Configure Real Motor

    This message is sent from DCSS to configure a real motor.

    This message will be accompanied by 13 fields:
    1.  the name of the motor to configure
    2.  the scaled position of the motor
    3.  the upper limit for the motor in scaled units
    4.  the lower limit for the motor in scaled units
    5.  the scale factor relating scaled units to steps for the motor
    6.  the slew rate for the motor in steps/sec
    7.  the acceleration time for the motor in seconds
    8.  the backlash amount for the motor in steps
    9.  is a boolean (0 or 1) indicating if the lower limit is enabled
    10. is a boolean (0 or 1) indicating if the upper limit is enabled
    11. is a boolean (0 or 1) indicating if the motor is software locked
    12. is a boolean (0 or 1) indicating if backlash correction is enabled
    13. is a boolean (0 or 1) indicating if the motor direction is reversed
    """
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_position(self):
        return self.get_args()[1]

    def get_motor_upperLimit(self):
        return self.get_args()[2]

    def get_motor_lowerLimit(self):
        return self.get_args()[3]

    def get_motor_scaleFactor(self):
        return self.get_args()[4]

    def get_motor_speed(self):
        return self.get_args()[5]

    def get_motor_acceleration(self):
        return self.get_args()[6]

    def get_motor_backlash(self):
        return self.get_args()[7]

    def get_motor_lowerLimitOn(self):
        return self.get_args()[8]

    def get_motor_upperLimitOn(self):
        return self.get_args()[9]

    def get_motor_motorLockOn(self):
        return self.get_args()[10]

    def get_motor_backlashOn(self):
        return self.get_args()[11]

    def get_motor_reverseOn(self):
        return self.get_args()[12]

@register_message('stoh_configure_pseudo_motor', 'dcss')
class DcssStoHConfigurePseudoMotor(DcssStoCMessage):
    """Server To Hardware Configure Pseudo Motor

    This message is sent from DCSS to configure a pseudo motor.

    This message will be accompanied by 7 fields:
    1.  the name of the motor to configure
    2.  the scaled position of the motor
    3.  the upper limit for the motor in scaled units
    4.  the lower limit for the motor in scaled units
    5.  is a boolean (0 or 1) indicating if the lower limit is enabled
    6. is a boolean (0 or 1) indicating if the upper limit is enabled
    7. is a boolean (0 or 1) indicating if the motor is software locked
    """
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_position(self):
        return self.get_args()[1]

    def get_motor_upperLimit(self):
        return self.get_args()[2]

    def get_motor_lowerLimit(self):
        return self.get_args()[3]

    def get_motor_lowerLimitOn(self):
        return self.get_args()[4]

    def get_motor_upperLimitOn(self):
        return self.get_args()[5]

    def get_motor_motorLockOn(self):
        return self.get_args()[6]

@register_message('stoh_set_motor_position', 'dcss')
class DcssStoHSetMotorPosition(DcssStoCMessage):
    """Server To Hardware Set Motor Position

    This message requests that the position of the specified motor be set to specified scaled value.

    This message takes two arguments:
    1. the name of the motor to configure.
    2. the new scaled position of the motor.
    """
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_position(self):
        return self.get_args()[1]

@register_message('stoh_start_motor_move', 'dcss')
class DcssStoHStartMotorMove(DcssStoCMessage):
    """This is a command to start a motor move.
    
    This message requests that the specified motor be moved to the specified scaled position.

    This message takes two arguments:
    1. the name of the motor to move
    2. the scaled destination of the motor.

    """
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_position(self):
        return self.get_args()[1]

@register_message('stoh_abort_all', 'dcss')
class DcssStoHAbortAll(DcssStoCMessage):
    """Server To Hardware Abort All

    This message requests that all operations either cease immediately or halt as soon as possible.

    This message takes one argument:
    1. either "hard" or "soft" e.g. htos_abort_all [hard | soft]

    All motors are stopped and data collection begins shutting down and stopping detector activity.
    A single argument specifies how motors should be aborted.
    A value of hard indicates that motors should stop without decelerating.
    A value of soft indicates that motors should decelerate properly before stopping.
    """
    def __init__(self, split):
        super().__init__(split)

    def get_abort_arg(self):
        return self.get_args()[0]

@register_message('stoh_correct_motor_position', 'dcss')
class DcssStoHCorrectMotorPosition(DcssStoCMessage):
    """Server To Hardware Correct Motor Position

    Requests that the position of the specified motor be adjusted by the specified correction.
    This is used to support the circle parameter for motors (i.e., modulo 360 behavior for a phi axis).
    
    This command takes two arguments:
    1. the name of the motor.
    2. the correction to be applied to the motor position.
    """
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_correction(self):
        return self.get_args()[1]

@register_message('stoh_set_motor_dependency', 'dcss')
class DcssStoHSetMotorDependency(DcssStoCMessage):
    """Server To Hardware Set Motor Dependency
    """
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_dependencies(self):
        return self.get_args()[1:]

@register_message('stoh_set_motor_children', 'dcss')
class DcssStoHSetMotorChildren(DcssStoCMessage):
    """Server To Hardware Set Motor Children
    """
    def __init__(self, split):
        super().__init__(split)

    def get_motor_name(self):
        return self.get_args()[0]

    def get_motor_children(self):
        return self.get_args()[1:]

@register_message('stoh_set_shutter_state','dcss')
class DcssStoHSetShutterState(DcssStoCMessage):
    """Server To Hardware Set Shutter State

    This message requests that the state of the specified shutter or filter be changed to a particular state.

    This message is accompanied by one argument:
    1. the desired state (open or closed)
    """
    def __init__(self, split):
        super().__init__(split)

    def get_shutter_name(self):
        return self.get_args()[0]

    def get_shutter_state(self):
        return self.get_args()[1]

@register_message('stoh_start_operation','dcss')
class DcssStoHStartOperation(DcssStoCMessage):
    """Server To Hardware Start Operation

    This message requests that the DHS start an operation.

    This message is accompanied by three arguments:
    1. the name of the operation to be started.
    2. is a unique handle currently constructed by calling the create_operation_handle procedure in BLU-ICE.
       This currently creates a handle in the following format:
        
    clientNumber.operationCounter

    where clientNumber is a unique number provided by DCSS for each connected GUI or Hardware client.
    DCSS will reject an operation message if the clientNumber does not match the client.
    The operationCounter is a number that the client should increment with each new operation that is started.

    3. is the list of arguments that should be passed to the operation.
       It is recommended that the list of arguments continue to follow the general format of the DCS message structure (space separated tokens).
    However, this requirement can only be enforced by the writer of the operation handlers.

    The message requests DCSS to forward the request to the appropriate hardware server.
    """
    def __init__(self, split):
        super().__init__(split)

    def get_operation_name(self):
        return self.get_args()[0]

    def get_operation_handle(self):
        return self.get_args()[1]

    def get_operation_args(self):
        return self.get_args()[2:]

# --------------------------------------------------------------------------
# Messages Outgoing to DCSS (Hardware TO Server)
@register_message('htos_client_is_hardware')
class DcssHtoSClientIsHardware(DcssHtoSMessage):
    """Hardware To Server Clinet Is Hardware

    This should be sent by all hardware servers in response to stoc_send_client_type message from DCSS.

    The format of the message is

    htos_client_is_hardware dhsName

    where:

    Where dhsName is the name of a hardware server listed within the database.dat file as described in DHS entry definition .

    Note: This message is not forwarded to the GUI clients.
    """
    def __init__(self, dhs_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), dhs_name]

@register_message('htos_motor_move_started')
class DcssHtoSMotorMoveStarted(DcssHtoSMessage):
    """Hardware To Server Motor Move Started

    This message indicates that the requested move of a motor has begun.
    This message is forwarded by DCSS to all GUI clients as a stog_motor_move_started message.

    The format of the message is:

    htos_motor_move_started motorName position

    where:

    motorName is the name of the motor.
    position is the destination move the motor.
    """
    def __init__(self, motor_name:str, new_position:float):
        super().__init__()
        self._split_msg = [self.get_type_id(), motor_name, new_position]

@register_message('htos_motor_move_completed')
class DcssHtoSMotorMoveCompleted(DcssHtoSMessage):
    """Hardware To Server Motor Move Completed

    Indicates that the move on the specified motor is now complete.
    DCSS forwards this message to all GUI clients as a stog_motor_move_completed message.

    The format of the message is:

    htos_motor_move_completed motorName position completionStatus

    where:

    motorName is the name of the motor that finished the move.
    position is the final position of the motor.
    completionStatus is the status of the motor with values as shown below:
        normal indicates that the motor finished its commanded move successfully.
        aborted indicates that the motor move was aborted.
        moving indicates that the motor was already moving.
        cw_hw_limit indicates that the motor hit the clockwise hardware limit.
        ccw_hw_limit indicates that the motor hit the counter-clockwise hardware limit.
        both_hw_limits indicates that the motor cable may be disconnected.
        unknown indicates that the motor completed abnormally, but the DHS software or the hardware controller does not know why.
    """
    def __init__(self, motor_name:str, new_position:float, state:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), motor_name, new_position, state]

@register_message('htos_operation_completed')
class DcssHtoSOperationCompleted(DcssHtoSMessage):
    """Hardware To Server Operation Completed

    The message is used to indicate that an operation has been completed by this hardware server.
    
    The general format of the message is:

    htos_operation_completed operationName operationHandle status arguments

    where:

    operationName: is the name of the operation that completed.
    operationHandle: is the unique value that indicates which instance of the operation completed.
    status: Anything other than a normal in this field will indicate to DCSS and BLU-ICE that the operation failed, and this token will become the reason of failure.
    arguments: This is a list of return values.
               It is recommended that list of return arguments adhere to the overall DCS protocol (space separated tokens), but this can only be enforced by the writer of the operation handle.
    """
    def __init__(self, operation_name:str, operation_handle:float, operation_status:str, operation_args:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), operation_name, operation_handle, operation_status, operation_args]

@register_message('htos_operation_update')
class DcssHtoSOperationUpdate(DcssHtoSMessage):
    """Hardware To Server Operation Update

    This message can be used to send small pieces of data to the GUI clients as progress is made on the operation.
    It can also be used to indicate to a calling GUI client that the operation cannot continue until the caller performs another task.

    The message format is as follows:

    htos_operation_update operationName operationHandle arguments

    where:

    operationName: is the name of the operation that completed.
    operationHandle: is the unique value that indicates which instance of the operation completed.
    arguments: This is a list of return values.
               It is recommended that list of return arguments adhere to the overall DCS protocol (space separated tokens), but this can only be enforced by the writer of the operation handle.
    """
    def __init__(self, operation_name:str, operation_handle:float, operation_args:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), operation_name, operation_handle, operation_args]

@register_message('htos_start_operation')
class DcssHtoSStartOperation(DcssHtoSMessage):
    """Hardware To Server Start Operation
    """
    def __init__(self, operation_info:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), operation_info]

@register_message('htos_update_motor_position')
class DcssHtoSUpdateMotorPosition(DcssHtoSMessage):
    """Hardware To Server Update Motor Position
    """
    def __init__(self, motor_name:str, new_position:float, status:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), motor_name, new_position, status]

@register_message('htos_report_ion_chambers')
class DcssHtoSReportIonChamber(DcssHtoSMessage):
    """Hardware To Server Report Ion Chamber

    This message reports the results of counting on one or more ion chambers in response to the stog_read_ion_chamber message.
    The first three arguments are mandatory. Additional ion chambers are reported by adding additional arguments.
    DCSS forwards this message to all GUI clients as stog_report_ion_chambers message.

    The format of the message is:

    htos_report_ion_chambers time ch1 counts1 [ch2 counts2 [ch3 counts3 [chN countsN]]]

    where:

    time: is the time in seconds over which counts were integrated.
    ch1: is the name of the first ion chamber read.
    cnts1: is the counts from the first ion chamber.
    """
    def __init__(self, ion_chamber_counts:int):
        super().__init__()
        self._split_msg = [self.get_type_id(), ion_chamber_counts]

@register_message('htos_configure_device')
class DcssHtoSConfigureDevice(DcssHtoSMessage):
    """Hardware To Server Configure Device

    The format of the message can take one of two forms:

    1. gtos_configure_device motorName position upperLimit lowerLimit lowerLimitOn upperLimitOn motorLockOn units

    where:

    motorName is the name of the motor to configure.
    position is the scaled position of the motor.
    upperLimit is the upper limit for the motor in scaled units.
    lowerLimit is the lower limit for the motor in scaled units.
    lowerLimitOn is a boolean (0 or 1) indicating if the lower limit is enabled.
    upperLimitOn is a boolean (0 or 1) indicating if the upper limit is enabled.
    motorLockOn is a boolean (0 or 1) indicating if the motor is software locked.

    2. gtos_configure_device motoName position upperLimit lowerLimit scaleFactor speed acceleration backlash lowerLimitOn upperLimitOn motorLockOn backlashOn reverseOn

    where:

    motor is the name of the motor to configure
    position is the scaled position of the motor
    upperLimit is the upper limit for the motor in scaled units
    lowerLimit is the lower limit for the motor in scaled units
    scaleFactor is the scale factor relating scaled units to steps for the motor
    speed is the slew rate for the motor in steps/sec
    acceleration is the acceleration time for the motor in seconds
    backlash is the backlash amount for the motor in steps
    lowerLimitOn is a boolean (0 or 1) indicating if the lower limit is enabled
    upperLimitOn is a boolean (0 or 1) indicating if the upper limit is enabled
    motorLockOn is a boolean (0 or 1) indicating if the motor is software locked
    backlashOn is a boolean (0 or 1) indicating if backlash correction is enabled
    reverseOn is a boolean (0 or 1) indicating if the motor direction is reversed

    This command requests that the configuration of a real motor be changed.
    DCSS updates the device configuration in its internal database (database.dat) and forwards the message to the appropriate hardware server.

    Note: This message should probably be two separate messages gtos_configure_real_motor and gtos_configure_pseudo_motor.
    """
    def __init__(self, device_name:str, device_settings:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), device_name, device_settings]

@register_message('htos_send_configuration')
class DcssHtoSSendConfiguration(DcssHtoSMessage):
    """Hardware To Server Send Configuration

    This message requests that the configuration of the specified device (as remembered by DCSS) be returned to this DHS.
    DCSS will respond with a stoh_configure_device message for the device. This message is not forwarded to the GUI clients.

    The format of the message is:

    htos_send_configuration deviceName

    where:

    deviceName is the name of the device for which the configuration information is needed.
    """
    def __init__(self, device_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), device_name]

@register_message('htos_report_shutter_state')
class DcssHtoSReportShutterState(DcssHtoSMessage):
    """Hardware To Server Report Shutter State

    This message reports a change in the state of a shutter.
    This may occur as a result of handling the stoh_set_shutter_state command or during a timed exposure with automated shutter handling.
    DCSS forwards this message to all GUI clients as a stog_report_shutter_state message.

    The format of the message is:

    htos_report_shutter_state shutterName state

    where:

    shutterName is the name of the shutter.
    state is the new state (open | closed.)
    """
    def __init__(self, shutter_name:str, state:str, result:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), shutter_name, state, result]


@register_message('htos_limit_hit')
class DcssHtoSLimitHit(DcssHtoSMessage):
    """Hardware To Server Limit Hit
    """
    def __init__(self, state:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), state]

@register_message('htos_simulating_device')
class DcssHtoSSimulatingDevice(DcssHtoSMessage):
    """Hardware To Server Simulating Device
    """
    def __init__(self, state:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), state]

@register_message('htos_motor_correct_started')
class DcssHtoSMotorCorrectStarted(DcssHtoSMessage):
    """Hardware To Server Motor Correct Started
    """
    def __init__(self, state:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), state]

@register_message('htos_get_encoder_completed')
class DcssHtoSGetEncoderCompleted(DcssHtoSMessage):
    """Hardware To Server Get Encoder Completed
    """
    def __init__(self, encoder_name:str, new_position:float, status:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), encoder_name, new_position, status]

@register_message('htos_set_encoder_completed')
class DcssHtoSSetEncoderCompleted(DcssHtoSMessage):
    """Hardware To Server Set Encoder Completed
    """
    def __init__(self, encoder_name:str, new_position:int, status:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), encoder_name, new_position, status]

@register_message('htos_set_string_completed')
class DcssHtoSSetStringCompleted(DcssHtoSMessage):
    """Hardware To Server Set String Completed
    """
    def __init__(self, string_name:str, status:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), string_name, status]

@register_message('htos_note')
class DcssHtoSNote(DcssHtoSMessage):
    """Hardware To Server Note
    """
    def __init__(self, note_message:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), note_message]

@register_message('htos_log')
class DcssHtoSLog(DcssHtoSMessage):
    """Hardware To Server Log
    """
    def __init__(self, log_message:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), log_message]

@register_message('htos_set_motor_message')
class DcssHtoSSetMotorMessage(DcssHtoSMessage):
    """Hardware To Server Set Motor Message
    """
    def __init__(self, motor_name:str):
        super().__init__()
        self._split_msg = [self.get_type_id(), motor_name]


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
