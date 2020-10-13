import logging
from enum import Enum
from pydhsfw.threads import AbortableThread
from pydhsfw.messages import IncomingMessageQueue, OutgoingMessageQueue, MessageIn, MessageOut, MessageFactory
from pydhsfw.transport import Transport

_logger = logging.getLogger(__name__)

class ConnectionMessage(MessageIn):

    def __init__(self, msg):
        super().__init__()
        self._msg = msg

    def get_msg(self):
        return self._msg

class ConnectionConnectedMessage(ConnectionMessage):
    def __init__(self, msg):
        super().__init__(msg)

class ConnectionDisconnectedMessage(ConnectionMessage):
    def __init__(self, msg):
        super().__init__(msg)

class ConnectionShutdownMessage(ConnectionMessage):
    def __init__(self, msg):
        super().__init__(msg)

class MessageReader():
    def read(self):
        pass

class Connection:

    def __init__(self, connection_name:str, url:str, config:dict={}):
        self._connection_name = connection_name
        self._url = url
        self._config = config

    def connect(self):
        pass

    def disconnect(self):
        pass

    def send(self, msg:MessageOut):
        """
        DO docstrings show stuff in MS VSCODE When I hover over the command?
        """
        pass

    def shutdown(self):
        pass

    def wait(self):
        pass

class ConnectionRegistry():

    _registry = {}

    @classmethod
    def _register_connection(cls, connection_scheme:str, connection_cls: Connection):
        cls._registry[connection_scheme]=connection_cls

    @classmethod
    def _get_connection_class(cls, connection_scheme:str):
        return cls._registry.get(connection_scheme, None)        

    @classmethod
    def _get_connection_classes(cls):
        return cls._registry

def register_connection(connection_scheme:str):
    def decorator_register_connection(cls):
        cls._scheme = connection_scheme
        if connection_scheme and issubclass(cls, Connection):
            ConnectionRegistry._register_connection(connection_scheme, cls)
        
        return cls

    return decorator_register_connection


class ConnectionReadWorker(AbortableThread):
    
    def __init__(self, connection_name:str, transport:Transport, incoming_message_queue:IncomingMessageQueue, message_factory:MessageFactory, config:dict={}):
        super().__init__(name=f'{connection_name} connection read worker', config=config)
        self._connection_name = connection_name
        self._transport = transport
        self._msg_queue = incoming_message_queue
        self._msg_factory = message_factory

    def run(self):

        # Only one thing to do here, read raw messages from the transport and translate them 
        # into messages using the message factory, then stick them on the incoming queue.

        try:
            while True:
                try:
                    # Blocking call with timeout configured elsewhere. This will timeout to check for control messages, specifically SystemExit.
                    raw_msg = self._transport.receive()
                    if raw_msg:
                        _logger.debug(f'Received unpacked raw message, {raw_msg}')
                        msg = self._msg_factory.create_message(raw_msg)
                        if msg:
                            _logger.debug(f'Received factory created message: {msg}')
                            self._msg_queue.queue(msg)

                except TimeoutError:
                    #Socket read timed out. This is normal, it just means that no messages have been sent so we can ignore it.
                    pass
                except Exception:
                    # Log other exceptions so we can monitor and create special handlng if needed.
                    _logger.exception(None)
                    raise

        except SystemExit:
            _logger.info(f'Shutdown signal received, exiting {self.name}')
            
        finally:
            pass

class ConnectionWriteWorker(AbortableThread):
    
    def __init__(self, connection_name:str, transport:Transport, outgoing_message_queue:OutgoingMessageQueue, config:dict={}):
        super().__init__(name=f'{connection_name} connection write worker', config=config)
        self._connection_name = connection_name
        self._transport = transport
        self._msg_queue = outgoing_message_queue

    def run(self):

        # Only one thing to do here, read messages from the queue and write raw message to the transport

        try:
            while True:
                try:
                    # Blocking call with timeout configured elsewhere. This will timeout to check for control messages, specifically SystemExit.
                    msg = self._msg_queue.fetch(self._get_blocking_timeout())
                    if msg:
                        _logger.debug(f"Sending message: {msg}")
                        buffer = msg.write()
                        _logger.debug(f"Sending unpacked raw message: {buffer}")
                        self._transport.send(buffer)

                except TimeoutError:
                    #Socket read timed out. This is normal, it just means that no messages have been sent so we can ignore it.
                    pass
                except Exception:
                    # Log other exceptions so we can monitor and create special handlng if needed.
                    _logger.exception(None)
                    raise

        except SystemExit:
            _logger.info(f'Shutdown signal received, exiting {self.name}')
            
        finally:
            pass

class ConnectionBase(Connection):
    def __init__(self, connection_name:str, url:str, transport:Transport, incoming_message_queue:IncomingMessageQueue, outgoing_message_queue:OutgoingMessageQueue, message_factory:MessageFactory, config:dict={}):
        super().__init__(url, config)
        self._transport = transport
        self._read_worker = ConnectionReadWorker(connection_name, transport, incoming_message_queue, message_factory, config)
        self._write_worker = ConnectionWriteWorker(connection_name, transport, outgoing_message_queue, config)
        self._outgoing_message_queue = outgoing_message_queue
        self._transport.start()
        self._read_worker.start()
        self._write_worker.start()

    def connect(self):
        self._transport.connect()

    def disconnect(self):
        self._transport.disconnect()

    def send(self, msg:MessageOut):
        self._outgoing_message_queue.queue(msg)

    def shutdown(self):
        self._read_worker.abort()
        self._write_worker.abort()
        self._transport.shutdown()

    def wait(self):
        self._read_worker.join()
        self._write_worker.join()
        self._transport.wait()
