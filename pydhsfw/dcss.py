# -*- coding: utf-8 -*-
import logging
from typing import Any
from inspect import isfunction, signature, getsourcelines, getmodule
from pydhsfw.messages import (
    IncomingMessageQueue,
    OutgoingMessageQueue,
    MessageIn,
    MessageOut,
    MessageFactory,
    register_message,
)
from pydhsfw.transport import (
    MessageStreamReader,
    MessageStreamWriter,
    StreamReader,
    StreamWriter,
)
from pydhsfw.connection import ConnectionBase, register_connection
from pydhsfw.tcpip import TcpipClientTransport
from pydhsfw.processors import Context, MessageQueueDispatcher

_logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# DCSS Message Base Classes
class DcssMessageIn:
    def __init__(self, split):
        self._split_msg = split

    def __str__(self):
        return ' '.join(self._split_msg)

    @staticmethod
    def _split(buffer: bytes):
        return buffer.decode('ascii').rstrip('\n\r\x00').split(' ')

    @staticmethod
    def parse_type_id(buffer: bytes):
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
    def parse(cls, buffer: bytes):

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
        return ' '.join(self._split_msg)

    def write(self) -> bytes:
        buffer = None

        if self._split_msg:
            buffer = ' '.join(self._split_msg).encode('ascii')

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
    """Server To Hardware Register Real Motor

    This message is received from DCSS to tell the DHS which real motor(s) it is responsible for.

    This message will be accompanied by two fields:
    1. the name of the motor as it is known in the DCSS database
    2. the name of the motor as it is known in the DHS code

    note: these are almost always identical
    """

    def __init__(self, split):
        super().__init__(split)

    @property
    def motor_name(self):
        return self.args[0]

    @property
    def motor_hardwareName(self):
        return self.args[1]


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

    @property
    def string_name(self):
        return self.args[0]

    @property
    def string_hardwareName(self):
        return self.args[1]


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

    @property
    def encoder_name(self):
        return self.args[0]

    @property
    def encoder_hardwareName(self):
        return self.args[1]


@register_message('stoh_register_object', 'dcss')
class DcssStoHRegisterObject(DcssStoCMessage):
    """Server to Hardware Register Object

    Not sure if this is used or what it is used for.
    """

    def __init__(self, split):
        super().__init__(split)

    @property
    def object_name(self):
        return self.args[0]

    @property
    def object_hardwareName(self):
        return self.args[1]


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
    """Server To Hardware Set Motor Position

    This message requests that the position of the specified motor be set to specified scaled value.

    This message takes two arguments:
    1. the name of the motor to configure.
    2. the new scaled position of the motor.
    """

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
    """This is a command to start a motor move.

    This message requests that the specified motor be moved to the specified scaled position.

    This message takes two arguments:
    1. the name of the motor to move
    2. the scaled destination of the motor.

    """

    def __init__(self, split):
        super().__init__(split)

    @property
    def motor_name(self):
        return self.args[0]

    @property
    def motor_position(self):
        return self.args[1]


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

    @property
    def abort_arg(self):
        return self.args[0]


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

    @property
    def motor_name(self):
        return self.args[0]

    @property
    def motor_correction(self):
        return self.args[1]


@register_message('stoh_set_motor_dependency', 'dcss')
class DcssStoHSetMotorDependency(DcssStoCMessage):
    """Server To Hardware Set Motor Dependency"""

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
    """Server To Hardware Set Motor Children"""

    def __init__(self, split):
        super().__init__(split)

    @property
    def motor_name(self):
        return self.args[0]

    @property
    def motor_children(self):
        return self.args[1:]


@register_message('stoh_set_shutter_state', 'dcss')
class DcssStoHSetShutterState(DcssStoCMessage):
    """Server To Hardware Set Shutter State

    This message requests that the state of the specified shutter or filter be changed to a particular state.

    This message is accompanied by one argument:
    1. the desired state (open or closed)
    """

    def __init__(self, split):
        super().__init__(split)

    @property
    def shutter_name(self):
        return self.args[0]

    @property
    def shutter_state(self):
        return self.args[1]


@register_message('stoh_start_operation', 'dcss')
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

    @property
    def operation_name(self):
        return self.args[0]

    @property
    def operation_handle(self):
        return self.args[1]

    @property
    def operation_args(self):
        return self.args[2:]


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

    def __init__(self, dhs_name: str):
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

    def __init__(self, motor_name: str, new_position: float):
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

    def __init__(self, motor_name: str, new_position: float, state: str):
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

    def __init__(
        self,
        operation_name: str,
        operation_handle: float,
        operation_status: str,
        operation_args: str,
    ):
        super().__init__()
        self._split_msg = [
            self.get_type_id(),
            operation_name,
            operation_handle,
            operation_status,
            operation_args,
        ]


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

    def __init__(
        self, operation_name: str, operation_handle: float, operation_args: str
    ):
        super().__init__()
        self._split_msg = [
            self.get_type_id(),
            operation_name,
            operation_handle,
            operation_args,
        ]


@register_message('htos_start_operation')
class DcssHtoSStartOperation(DcssHtoSMessage):
    """Hardware To Server Start Operation"""

    def __init__(self, operation_info: str):
        super().__init__()
        self._split_msg = [self.get_type_id(), operation_info]


@register_message('htos_update_motor_position')
class DcssHtoSUpdateMotorPosition(DcssHtoSMessage):
    """Hardware To Server Update Motor Position"""

    def __init__(self, motor_name: str, new_position: float, status: str):
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

    def __init__(self, ion_chamber_counts: int):
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

    def __init__(self, device_name: str, device_settings: str):
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

    def __init__(self, device_name: str):
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

    def __init__(self, shutter_name: str, state: str, result: str):
        super().__init__()
        self._split_msg = [self.get_type_id(), shutter_name, state, result]


@register_message('htos_limit_hit')
class DcssHtoSLimitHit(DcssHtoSMessage):
    """Hardware To Server Limit Hit"""

    def __init__(self, state: str):
        super().__init__()
        self._split_msg = [self.get_type_id(), state]


@register_message('htos_simulating_device')
class DcssHtoSSimulatingDevice(DcssHtoSMessage):
    """Hardware To Server Simulating Device"""

    def __init__(self, state: str):
        super().__init__()
        self._split_msg = [self.get_type_id(), state]


@register_message('htos_motor_correct_started')
class DcssHtoSMotorCorrectStarted(DcssHtoSMessage):
    """Hardware To Server Motor Correct Started"""

    def __init__(self, state: str):
        super().__init__()
        self._split_msg = [self.get_type_id(), state]


@register_message('htos_get_encoder_completed')
class DcssHtoSGetEncoderCompleted(DcssHtoSMessage):
    """Hardware To Server Get Encoder Completed"""

    def __init__(self, encoder_name: str, new_position: float, status: str):
        super().__init__()
        self._split_msg = [self.get_type_id(), encoder_name, new_position, status]


@register_message('htos_set_encoder_completed')
class DcssHtoSSetEncoderCompleted(DcssHtoSMessage):
    """Hardware To Server Set Encoder Completed"""

    def __init__(self, encoder_name: str, new_position: int, status: str):
        super().__init__()
        self._split_msg = [self.get_type_id(), encoder_name, new_position, status]


@register_message('htos_set_string_completed')
class DcssHtoSSetStringCompleted(DcssHtoSMessage):
    """Hardware To Server Set String Completed"""

    def __init__(self, string_name: str, status: str):
        super().__init__()
        self._split_msg = [self.get_type_id(), string_name, status]


@register_message('htos_note')
class DcssHtoSNote(DcssHtoSMessage):
    """Hardware To Server Note"""

    def __init__(self, note_message: str):
        super().__init__()
        self._split_msg = [self.get_type_id(), note_message]


@register_message('htos_log')
class DcssHtoSLog(DcssHtoSMessage):
    """Hardware To Server Log"""

    def __init__(self, log_message: str):
        super().__init__()
        self._split_msg = [self.get_type_id(), log_message]


@register_message('htos_set_motor_message')
class DcssHtoSSetMotorMessage(DcssHtoSMessage):
    """Hardware To Server Set Motor Message"""

    def __init__(self, motor_name: str):
        super().__init__()
        self._split_msg = [self.get_type_id(), motor_name]


# Message Factory
class DcssMessageFactory(MessageFactory):
    def __init__(self):
        super().__init__('dcss')

    def _parse_type_id(self, raw_msg: bytes):
        return DcssMessageIn.parse_type_id(raw_msg)


class DcssDhsV1MessageReader(MessageStreamReader):
    def __init__(self):
        pass

    def read_msg(self, stream_reader: StreamReader) -> bytes:
        packed = stream_reader.read(200)
        _logger.debug(f'Received packed raw message: {packed}')
        return packed.decode('ascii').rstrip('\n\r\x00').encode('ascii')


class DcssDhsV1MessageWriter(MessageStreamWriter):
    def __init__(self):
        pass

    def write_msg(self, stream_writer: StreamWriter, msg: bytes):
        packed = msg.decode('ascii').ljust(200, '\x00').encode('ascii')
        _logger.debug(f'Sending packed raw message: {packed}')
        stream_writer.write(packed)


class DcssDhsV2MessageReaderWriter(MessageStreamReader, MessageStreamWriter):
    def __init__(self):
        super(MessageStreamReader).__init__()
        super(MessageStreamWriter).__init__()
        self._version = 1

    def read_msg(self, stream_reader: StreamReader) -> bytes:

        unpacked = None

        header = stream_reader.read(26)
        packed = header
        hdr_str = header.decode('ascii').rstrip('\x00\r\n').split()
        if hdr_str[0].isnumeric():
            text_size = int(hdr_str[0])
            bin_size = int(hdr_str[1])
            text_buf = stream_reader.read(text_size)
            packed += text_buf
            bin_buf = stream_reader.read(bin_size)
            if bin_buf:
                packed += bin_buf

            unpacked = text_buf
            self._version = 2
        else:
            tailer = stream_reader.read(174)
            packed += tailer
            unpacked = packed
            self._version = 1

        _logger.debug(f'Received packed raw message version {self._version}: {packed}')

        return unpacked.decode('ascii').rstrip('\n\r\x00').encode('ascii')

    def write_msg(self, stream_writer: StreamWriter, msg: bytes):

        packed = None
        if self._version == 2:
            msg_str = msg.decode('ascii').rstrip('\r\n\x00') + ' \x00'
            msg_text_buf = msg_str.encode('ascii')
            msg_text_len = len(msg_text_buf)
            msg_header = str(msg_text_len).rjust(12) + str(0).rjust(13) + ' '
            msg_hdr_buf = msg_header.encode('ascii')
            packed = msg_hdr_buf + msg_text_buf
        else:
            packed = msg.decode('ascii').ljust(200, '\x00').encode('ascii')

        _logger.debug(f'Sending packed raw message version {self._version}: {packed}')
        stream_writer.write(packed)


class DcssClientTransport(TcpipClientTransport):
    def __init__(self, connection_name: str, url: str, config: dict = {}):
        self._msg_reader_writer = DcssDhsV2MessageReaderWriter()
        super().__init__(
            connection_name,
            url,
            self._msg_reader_writer,
            self._msg_reader_writer,
            config,
        )


@register_connection('dcss')
class DcssClientConnection(ConnectionBase):
    def __init__(
        self,
        connection_name: str,
        url: str,
        incoming_message_queue: IncomingMessageQueue,
        outgoing_message_queue: OutgoingMessageQueue,
        config: dict = {},
    ):
        super().__init__(
            connection_name,
            url,
            DcssClientTransport(connection_name, url, config),
            incoming_message_queue,
            outgoing_message_queue,
            DcssMessageFactory(),
            config,
        )


class DcssOperationHandlerRegistry:

    _default_processor_name = 'default'
    _registry = {}

    @classmethod
    def _register_start_operation_handler(
        cls, operation_name: str, operation_handler_function, processor_name: str = None
    ):
        if not processor_name:
            processor_name = cls._default_processor_name

        if not isfunction(operation_handler_function):
            raise TypeError('handler_function must be a function')

        hf_sig = signature(operation_handler_function)
        msg_param = hf_sig.parameters.get('message')
        if not issubclass(msg_param.annotation, DcssStoHStartOperation):
            raise TypeError(
                'The handler_function must have a named parameter "message" that is of type DcssStoHStartOperation'
            )

        ctx_param = hf_sig.parameters.get('context')
        if not issubclass(ctx_param.annotation, DcssContext):
            raise TypeError(
                'The handler_function must have a named parameter "context" that is of type DcssContext'
            )

        if cls._registry.get(processor_name) is None:
            cls._registry[processor_name] = dict()

        cls._registry[processor_name][operation_name] = operation_handler_function

    @classmethod
    def _get_operation_handlers(cls, processor_name: str = None):
        if not processor_name:
            processor_name = cls._default_processor_name
        return cls._registry.get(processor_name, {})


def register_dcss_start_operation_handler(
    operation_name: str, dispatcher_name: str = None
):
    """Registers a function to handle a dcss start operation message.

    operation_name - Start operations that match this operation name will be dispatched to this function.

    dispatcher_name - Name of the message dispatcher that will be routing the messages to this message handler.
    Each message dispatcher will run in it's own thread and in the future there may be a requirement
    to have multiple dispatchers. For now, there is only one dispatcher, so leave this blank or set
    it to None.

    The function signature must match:

    def handler(message:DcssStoHStartOperation, context:DcssContext)

    """

    def decorator_register_start_operation_handler(func):
        DcssOperationHandlerRegistry._register_start_operation_handler(
            operation_name, func, dispatcher_name
        )
        return func

    return decorator_register_start_operation_handler


class DcssActiveOperation:
    """Storage class for active operations.

    Active operations that are currently underway have information stored in this class. It stores the operation name, operation handle, start operation message,
    and a DHS implementation placeholder property called state that implementer can use to store whatever they need for the duration of the operation.

    This class is created and stored in the active operations list when a dcss stoh_start_operation message is received.
    It is removed, along with state allocations mentioned above, when the DHS responds with the htos_operation_completed message.
    After the call, the active operation and state is no longer available.

    """

    def __init__(
        self,
        operation_name: str,
        operation_handle: str,
        start_operation_message: DcssStoHStartOperation,
    ):
        self._operation_name = operation_name
        self._operation_handle = operation_handle
        self._start_operation_message = start_operation_message
        self._operation_state = None

    @property
    def operation_name(self):
        return self._operation_name

    @property
    def operation_handle(self):
        return self._operation_handle

    @property
    def start_operation_message(self):
        return self._start_operation_message

    @property
    def operation_state(self):
        return self._operation_state

    @operation_state.setter
    def operation_state(self, operation_state: Any):
        self._operation_state = operation_state


class DcssActiveOperations:
    """ Stores a list of active operations that are currently in progress"""

    def __init__(self):
        self._active_operations = []

    def add_operation(self, operation: DcssActiveOperation):
        # replace existing operation if there is one.
        self.remove_operation(operation)
        self._active_operations.append(operation)

    def get_operations(self, operation_name: str = None, operation_handle=None):
        return list(
            filter(
                lambda op: (not operation_name or operation_name == op.operation_name)
                and (not operation_handle or operation_handle == op.operation_handle),
                self._active_operations,
            )
        )

    def remove_operation(self, operation: DcssActiveOperation):
        ops = self.get_operations(operation.operation_name, operation.operation_handle)
        for op in ops:
            while op in self._active_operations:
                self._active_operations.remove(op)

    def remove_operations(self, operations: list):
        for op in operations:
            self.remove_operation(op)


class DcssContext(Context):
    def __init__(self, active_operations: DcssActiveOperations):
        super().__init__()
        self._active_operations = active_operations

    def get_active_operations(
        self, operation_name: str = None, operation_handle=None
    ) -> DcssActiveOperation:
        """Retrieve active operations that match the name and/or handle.

        operation_name - Get all active operations that match that name. None acts as a wildcard.
        operation_handle - Get all active operations that match that handle. None acts as a wildcard.

        Note: To get active operations pas in None for name and handle. To be specific you can match both
        name and handle.

        """
        return self._active_operations.get_operations(operation_name, operation_handle)

    def get_active_operation_names(self):
        return self._active_operations.get_operations()


class DcssOutgoingMessageQueue(OutgoingMessageQueue):
    def __init__(self, active_operations: DcssActiveOperations):
        super().__init__()
        self._active_operations = active_operations

    def queue(self, message: MessageOut):
        super().queue(message)

        # Special handling for operation completed messages
        if isinstance(message, DcssHtoSOperationCompleted):
            ops = self._active_operations.get_operations(
                message._split_msg[1], message._split_msg[2]
            )
            self._active_operations.remove_operations(ops)


class DcssMessageQueueDispatcher(MessageQueueDispatcher):
    def __init__(
        self,
        name: str,
        incoming_message_queue: IncomingMessageQueue,
        context: Context,
        active_operations: DcssActiveOperations,
        config: dict = {},
    ):
        super().__init__(name, incoming_message_queue, context, config)
        self._active_operations = active_operations
        self._operation_handler_map = (
            DcssOperationHandlerRegistry._get_operation_handlers()
        )

    def start(self):
        super().start()
        for op_name, func in self._operation_handler_map.items():
            lineno = getsourcelines(func)[1]
            module = getmodule(func)
            _logger.debug(
                f'Registered start operation handler: {op_name}, {module.__name__}:{func.__name__}():{lineno} with {self._disp_name} dispatcher'
            )

    def process_message(self, message: MessageIn):
        # Send to parent dispatcher to handle messages.
        super().process_message(message)

        # Special handling for start operation messages.
        if isinstance(message, DcssStoHStartOperation):
            handler = self._operation_handler_map.get(message.operation_name)
            if isfunction(handler):
                self._active_operations.add_operation(
                    DcssActiveOperation(
                        message.operation_name, message.operation_handle, message
                    )
                )
                handler(message, self._context)

    def process_message_now(self, message: MessageIn):
        # Send to parent dispatcher to handle messages.
        super().process_message(message)
