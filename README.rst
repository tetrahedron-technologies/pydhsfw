Python Distributed Hardware Server Framework
############################################

This project provides a general python-based framework for writing a Distributed Hardware Servers (DHS).

Description
===========

This code base will provide the necessary functionality to communicate with DCSS and be configurable for different peices of hardware. For example it could be configured to control hardware connected to a Raspberry Pi, run some python scripts, run specific DCSS operations that need to access web resources or serial-connected devices.

Installation
============

checkout the code from GitHub.

|  ``git clone git@github.com:tetrahedron-technologies/pydhsfw.git``
|  ``cd pydhsfw``

optionally setup and source a virtualenv.

|  ``virtualenv -p python3.8 .env``
|  ``source .env/bin/activate``
|  ``pip install --upgrade pip``

and install into local python environment.

|  ``pip install -e .``

Usage
=====

Details to come. Looking at the tests is probably the best way to understand how to implement a new DHS using the ``pydhsfw``.

General Framework for a DHS
===========================

* write in python.
* run on a beamline computer.
* establish port/socket to communicate with DCSS using ``dcs`` protocol.
* establish connections to the hardware/service you want to control or interface with.


DCSS Communications
===================

The beamline uses a distributed control system akin to a hub and spoke control model where the central hub is referred to as DCSS (Distributed Control System Server) and the spokes are called DHSs (Distributed Hardware Controllers). In order to write a new DHS we need to establish communications with DCSS.

DCSS communicates with DHS using the ``dcs`` protocol. All ``dcs`` messages are prefixed with a 4 character code that will tell you about the the direction of the message. For example:

| ``stoc_``  **s**\ erver **to** **c**\ lient for messages originating from DCSS and destined for a client (hardware or software).
| ``stoh_``  **s**\ erver **to** **h**\ ardware for messages originating from DCSS and destined for hardware (i.e. a DHS).
| ``htos_``  **h**\ ardware **to** **s**\ erver for messages originating from a DHS and destined for DCSS.
| ``gtos_``  **G**\ UI **to** **s**\ erver for messages originating from the Blu-Ice GUI and destined for DCSS.
| ``stog_``  **s**\ erver **to** **G**\ UI for messages originating from DCSS and destined for the Blu-Ice GUI.


More details can be found in the `DCS Admin Guide <https://github.com/dsclassen/pyDHS/blob/master/docs/DCSS_ADMIN_GUIDE.pdf>`_. This PDF documentation has not been updated since 2005, but it is still worth browsing if you intend to write a functioning DHS.

We've tried to summarize some of the more useful points below.

....

The ``dcs`` Protocol
====================

The messaging protocol used with DCSS/DHS/Blu-Ice control system.

``dcs`` messages come in 2 flavors:

1. ``dcs1`` messages are always 200 bytes in length.
2. ``dcs2`` messages can be up to 1024 bytes, but the first message from DCSS to the DHS and the first response from the DHS back to DCSS must be exactly 200 bytes ONLY.

The pydhsfw has been written to taken care of the details of these communications for you. Although not needed in order to make use of pydhsfw the details of these communications between DCSS and a DHS are outlined below.

....

Connect to DCSS
---------------------------------------------------------

Open a socket to the dcss server on port 14242
You will receive a 200 byte message:

``stoc_send_client_type\0\0\0\0\0\0\0\0\0...``

Read 200 bytes from the socket.
The trailing end of the string ("...") can be garbage, but is usually zeroes.

Respond with:

``htos_client_is_hardware DHS_NAME\0\0\0...``

The very first response must be padded to 200 bytes. Need at least one zero at the end of the meaningful text.

DCSS will then send messages about the different motors, shutters, ion guages, strings, and operations that it thinks this DHS is responsible for:

|  ``stoh_register_operation operationName1 operationName1\0\0\0...``
|  ``stoh_register_operation operationName2 operationName2\0\0\0...``
|  ``stoh_register_operation operationName3 operationName3\0\0\0...``

|  ``stoh_register_real_motor motor1 motor1\0\0\0...``
|  ``stoh_register_real_motor motor2 motor2\0\0\0...``

|  ``stoh_register_string string1 standardString\0\0\0...``
|  ``stoh_register_string string2 string2\0\0\0...``


It is also worth noting that DCSS can "go away" and it is important that the DHS be able to automagically re-establish the socket connection should this happen.

....

Configure motors, shutters, strings, ion gauges, and operations
---------------------------------------------------------------

Configure motors by sending an ``htos_configure_device`` command. For example:

``htos_configure_device energy 12398.42 20000 2000 1 100000 1 -600 0 0 1 0 0 0\0...``

Where:

======    ==============================    ===============================================================
field     value                             notes
======    ==============================    ===============================================================
1         |  ``htos_configure_device``      The xos command to configure a device.
2         |  ``energy``                     The name of the motor you are configuring.
3         |  ``12398.42``                   The current position of this motor.
4         |  ``20000``                      The forward limit (in motor base units)
5         |  ``2000``                       The reverse limit (in motor base units)
6         |  ``1``                          The motor scale factor (steps/unit)
7         |  ``100000``                     The maximum speed (steps/sec)
8         |  ``1``                          The maximum acceleration (milliseconds)
9         |  ``-600``                       The backlash magnitude and direction (steps).
10        |  ``0``                          Enable the forward limit.  "1" is enabled "0" is disabled.
11        |  ``0``                          Enable the reverse limit.  "1" is enabled "0" is disabled.
12        |  ``0``                          Lock the motor.  "1" is locked "0" unlocked
13        |  ``0``                          Enable anti-backlash movement.  "1" enabled "0" disabled
14        |  ``0``                          Reverse the motor direction.  "1" enabled "0" disabled
15        |  ``0``                          Circle mode. (might be used for gonio phi?)
======    ==============================    ===============================================================


You must pad the message up to 200 bytes and use a zero byte to end the meaningful string.
If you enable the limits (fields 10 & 11), then DCSS will not ask you to move this motor beyond the numbers listed in fields 4 & 5.

Configure shutters by sending an ``htos_configure_shutter`` command. For example:

|  ``htos_configure_shutter shutter open close open\0...``
|  or
|  ``htos_configure_shutter Se open close open\0...``

Where:

======    ==============================    ===============================================================
field     value                             notes
======    ==============================    ===============================================================
1         |  ``htos_configure_shutter``     | The xos command to configure a shutter.
2         |  ``shutter``                    | The name of the shutter you are configuring.
3         |  ``open``                       | The name for the "open" position of this shutter.
4         |  ``closed``                     | The name for the "closed" position of this shutter.
5         |  ``open``                       | The current position of this shutter.
======    ==============================    ===============================================================

Although you can get a away with using "in" and "out" or "on" and "off" for shutter devices, there are certain situations in DCSS where this doesn’t work, so just use "open" and "closed" for everything.  NOTE: it is "closed" and **NOT** "close".

Configure strings by sending an ``htos_set_string_completed`` command. For example:

a simple string with a single word:

|  ``htos_set_string_completed detectorType normal PILATUS6``

or a string with multiple key/value pairs

|  ``htos_set_string_completed detectorStatus normal TEMP 26.0 HUMIDITY 2.1 GAPFILL -1 EXPOSUREMODE null DISK_SIZE_KB 0 DISK_USED_KB 0 DISK_USE_PERCENT 0 FREE_IMAGE_SPACE 0 SUM_IMAGES false SUM_IMAGES_DELTA_DEG 0.1 N_FRAME_IMG 1 THRESHOLD 6330.0 GAIN autog THRESHOLD_SET false SETTING_THRESHOLD false``

Where:

======    ================================    ===============================================================
field     value                               notes
======    ================================    ===============================================================
1         |  ``htos_set_string_completed``    | The xos command to set a string in DCSS.
2         |  ``detectorType``                 | The name of the string you are configuring.
3         |  ``normal``                       | Tell DCSS that the string value was set successfully.
4         |  ``PILATUS6``                     | The value of the string.
======    ================================    ===============================================================


Strings are denoted as ``standardString`` or as mirror of teh stringname. I'm entirely clear on the importance or significance of this difference.

ion gauges and operations require no configuration.

....

Listen for messages from DCSS.
---------------------------------------------------------

These are the two important ones for a DHS that is performing operations only.

|  ``stoh_start_operation``
|  ``stoh_abort_all``

if controlling motors or shutter then need examples here.


The ``stoh_start_operation`` messages look like this
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

``stoh_start_operation operationName1 operationID arg1 arg2 .... argN``

Where:

======    ================================    ========================================================================
field     value                               notes
======    ================================    ========================================================================
1         |  ``operationName1``               |  The operation that DCSS has requested this DHS to execute.
2         |  ``operationID``                  |  A unique numeric ID used to keep track of this operation instance.
3         |  ``arg1 arg2 .... argN``          |  Optional set of args to pass into the DHS from DCSS.
======    ================================    ========================================================================

pyDHS can respond with periodic updates in the form of
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

``htos_operation_update operationName1 operationID updateMessage``

Where:

======    ================================    ========================================================================
field     value                               notes
======    ================================    ========================================================================
1         | ``operationName1``                |  The operation that DCSS has requested this DHS to execute.
2         | ``operationID``                   |  A unique numeric ID used to keep track of this operation instance.
3         | ``updateNessage``                 |  Any message you want to pass back to DCSS.
======    ================================    ========================================================================

and when the operation is completed with a message like this
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

``htos_operation_completed operationName1 operationID reason returnMessage``

======    ================================    ========================================================================
field     value                               notes
======    ================================    ========================================================================
1         | ``operationName1``                |  The operation that DCSS has requested this DHS to execute.
2         | ``operationID``                   |  A unique numeric ID used to keep track of this operation instance.
3         | ``reason``                        |  In theory can be anything, but normally would be `normal` or `error`
4         | ``updateMessage``                 |  Any additional info you want to pass back to DCSS.
======    ================================    ========================================================================


Still need more details for handling motors and shutter.

