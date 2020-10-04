import logging
from enum import Enum

_logger = logging.getLogger(__name__)

class TransportState(Enum):
    DISCONNECTED = 1
    CONNECTING = 2
    CONNECTED = 3
    DISCONNECTING = 4
    RECONNECTED = 5
    RECONNECTING = 6

class Transport:
    ''' Underlying bass class that all transports must derive from. '''

    def __init__(self, url:str, config:dict={}):
        self._url = url
        self._config = config

    def connect(self):
        ''' Connects to the specified resource and maintains a persistent connection. '''
        pass

    def disconnect(self):
        ''' Disconnects from a resource. '''
        pass

    def reconnect(self):
        ''' Disconnects from a resource, then reconnects.
        
        This may be required when an existing connection
        is interrupted and must be re-established by completely disconnecting and reconnecting.

        '''
        pass

    def send(self, msg:bytes):
        ''' Send the raw bytes of an entire message.

        '''
        pass

    def receive(self)->bytes:
        ''' Receive the raw bytes of an entire message.

        This is a blocking call and will wait for a message to arrive or until the timeout duration is reached.
        
        Raises TimeoutError if there are no messages to read but the timeout duration has been reached.
        Note: Most implementations can only set the timeout during initialization of the Transport object
        so this is will be a requirement for all implementations.

        '''
        pass

    def start(self):
        ''' Start any transport workers.

        Many Transports require a worker thread to maintain the connection and/or process messages.
        This will start the worker threads and initialize any resources.

        '''
        pass

    def shutdown(self):
        ''' Shutdown the connection.

        Many Transports require a worker thread to maintain the connection and/or process messages.
        This will shutdown the worker threads and cleanup any resources.

        Once the Transport has been shutdown it cannot be used again.
        '''
        pass

    def wait(self):
        ''' Waits for the connection to shutdown.

        This is a blocking call.
        '''
        pass


class StreamReader():
    def __init__(self):
        pass

    def read(self, msglen:int)->bytes:
        ''' Reads a chunk of bytes from the stream.

        This is a blocking call and the wait timeout is determine by the implementation and generally by the stream it wraps.
        
        Raises a TimeoutError if the is nothing to read from the stream and the timeout has been reached.
        Raises a ConnectionAbortedError if the stream has been unusually aborted or disconnected.
        '''
        pass

    @property
    def _connected(self):
        raise NotImplementedError

    @_connected.setter
    def _connected(self, is_connected:bool):
        ''' Notifies the stream reader when a connection has been established or dropped.
        
        is_connected - True if the stream has an active connection, False otherwise.

        Since the stream reader wraps a socket, tty, pipe, etc. stream and the connection of that stream is maintained
        by the Transport, the Transport must notify the stream reader when it is connected or disconnected. Specifically
        this is used to maintain the read() blocking call when the underlying stream is not connected as they only block
        when there is an actvie connection.
        '''
        raise NotImplementedError

class StreamWriter():
    def __init__(self):
        pass

    def write(self, buffer:bytes):
        ''' Writes a chunk of bytes to the stream.
        
        Raises a ConnectionAbortedError if the stream has been unusually aborted or disconnected.
        '''
        pass

class MessageStreamReader():
    def __init__(self):
        pass

    def read_msg(self, stream_reader:StreamReader)->bytes:
        ''' Read the raw bytes of an entire message from a stream.

        The underlying message packets may be a fixed length, or end in a delimeter, or have a message header that describes size.
        The derived class is responsible for implementing the specific requirements for message decoding.

        '''
        pass

class MessageStreamWriter():
    def __init__(self):
        pass

    def write_msg(self, stream_writer:StreamWriter, msg:bytes):
        ''' Write the message to the stream writer.

        The message packets may need to be padded to a fixed length, or require a delimeter to be added, or may require packing in
        a message header that includes the message size. The derived class is responsible for the particulars of the message encoding.

        '''
        pass



class TransportStream(Transport):
    ''' Abstract class for transports that use streams and implementations of MessageStreamReader and MessageStreamWriter.

        Derived classes will need to implement a MessageStreamReader/Writer for the specific stream. They will also
        need to implement transport connection and thread management. 


    '''
    def __init__(self, url:str, message_reader:MessageStreamReader, messsage_writer:MessageStreamWriter, config:dict=None):
        super().__init__(url, config)
        self._message_reader = message_reader
        self._message_writer = messsage_writer

        
    def send(self, msg:bytes):
        try:
            self._message_writer.write_msg(self._stream_writer, msg)
        except ConnectionAbortedError:
            #Connection is lost because the socket was closed, probably from the other side.
            #Block the socket event and queue a reconnect message.
            _logger.warning('Connection lost, attempting to reconnect')
            self.reconnect()

    def receive(self):
        try:
            return self._message_reader.read_msg(self._stream_reader)
        except TimeoutError:
            #Read timed out. This is normal, it just means that no messages have been sent so we can ignore it.
            pass
        except ConnectionAbortedError:
            #Connection is lost because the socket was closed, probably from the other side.
            #Block the socket event and queue a reconnect message.
            _logger.warning('Connection lost, attempting to reconnect')
            self.reconnect()

    
    
