import logging
from threading import Event
from inspect import isfunction, signature
from traceback import print_exc
from collections import deque
from pydhsfw.threads import AbortableThread
from pydhsfw.messages import MessageIn
from pydhsfw.connection import Connection

_logger = logging.getLogger(__name__)

class Context:
    def create_connection(self, connection_name:str, url:str)->Connection:
        pass

    def get_connection(self, connection_name:str)->Connection:
        pass

    def get_dhs_state(self)->object:
        pass

    def set_dhs_state(self)->object:
        pass

class MessageHandlerRegistry():

    _default_processor_name = 'default'
    _registry = {}

    @classmethod
    def _register_message_handler(cls, msg_type_id:str, msg_handler_function, processor_name:str=None):
        if not processor_name:
            processor_name = cls._default_processor_name

        if not isfunction(msg_handler_function):
            raise TypeError('handler_function must be a function')

        hf_sig = signature(msg_handler_function)
        msg_param = hf_sig.parameters.get('message')
        if not issubclass(msg_param.annotation, MessageIn):
            raise TypeError('The handler_function must have a named parameter "message" that is of type MessageIn')

        ctx_param = hf_sig.parameters.get('context')
        if not issubclass(ctx_param.annotation, Context):
            raise TypeError('The handler_function must have a named parameter "context" that is of type Context')

        if cls._registry.get(processor_name) == None:
            cls._registry[processor_name] = dict()
        
        cls._registry[processor_name][msg_type_id]=msg_handler_function

    @classmethod
    def _get_message_handlers(cls, processor_name:str=None):
        if not processor_name:
            processor_name = cls._default_processor_name
        return cls._registry.get(processor_name, {})        


def register_message_handler(msg_type_id:str, processor_name:str=None):
    def decorator_register_handler(func):
        MessageHandlerRegistry._register_message_handler(msg_type_id, func, processor_name)
        return func

    return decorator_register_handler

class BlockingMessageProcessor():

    def __init__(self):
        super().__init__()
        self._deque_message = deque()
        self._deque_event = Event()

    def _queque_message(self, message:MessageIn):
        #Append message and unblock
        self._deque_message.append(message)
        self._deque_event.set()

    def _get_message(self, timeout=None):
        
        msg = None

        #Block until items are available
        if not self._deque_event.wait(timeout):
            raise TimeoutError
        
        elif self._deque_message: 
            msg = self._deque_message.popleft()

        #If there are no more items, start blocking again
        if not self._deque_message:
            self._deque_event.clear()
        return msg

    def _clear_messages(self):
            self._deque_event.clear()
            self._deque_message.clear()
        
class MessageProcessorWorker(AbortableThread, BlockingMessageProcessor):

    def __init__(self, name):
        AbortableThread.__init__(self, name=name)
        BlockingMessageProcessor.__init__(self)

    def process_message(self, message:MessageIn):
        pass

    def run(self):
        try:
            while True:
                try:
                    #Can't wait forever in blocking call, need to enter loop to check for control messages, specifically SystemExit.
                    msg = self._get_message(5)
                    if msg:
                        _logger.info(f"Processing message: {msg}")
                        self.process_message(msg)
                except TimeoutError:
                    #This is normal when there are no more mesages in the queue and wait time has ben statisfied. Just ignore it.
                    pass
                except Exception:
                    _logger.exception(f"da fuq?")
                    raise

        except SystemExit:
            _logger.info(f'Shutdown signal received, exiting {self.name}')
            
        finally:
            pass

class MessageDispatcher(MessageProcessorWorker):
    def __init__(self, name:str, context:Context):
        super().__init__(f'dhs {name} message processor')
        self._handler_map = MessageHandlerRegistry._get_message_handlers()
        self._context = context

    def process_message(self, message:MessageIn):
        type_id = message.get_type_id()
        handler = self._handler_map.get(type_id)
        if isfunction(handler):
            handler(message, self._context)

