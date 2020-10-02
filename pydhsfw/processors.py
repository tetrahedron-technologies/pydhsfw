import logging
from threading import Event
from inspect import isfunction, signature, getsourcelines, getmodule
from pydhsfw.threads import AbortableThread
from pydhsfw.messages import MessageIn, MessageQueue
from pydhsfw.connection import Connection

_logger = logging.getLogger(__name__)

class Context:
    def create_connection(self, connection_name:str, url:str)->Connection:
        pass

    def get_connection(self, connection_name:str)->Connection:
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


def register_message_handler(msg_type_id:str, dispatcher_name:str=None):
    '''Registers a function to handle message instances of the specified type id.

    msg_type_id - This message handler will receive all messages that are of this message type.

    dispatcher_name - Name of the message dispatcher that will be routing the messages to the handler.
    Each message dispatcher will run in it's own thread and in the future there may be a requirement 
    to have multiple dispatchers. For now, there is only one dispatcher, so leave this blank or set
    it to None.

    The function signature must match:

    def handler(message:MessageIn, context:Context)

    To register a dcss server to client send client type message:

    @register_message_handler('stoc_send_client_type')
    
    def send_client_type_handler(message:DcssStoCSendClientType, context:Context)

    '''
    def decorator_register_handler(func):
        MessageHandlerRegistry._register_message_handler(msg_type_id, func, dispatcher_name)
        return func

    return decorator_register_handler


class MessageQueueWorker(AbortableThread):

    def __init__(self, name, incoming_message_queue:MessageQueue, config:dict={}):
        AbortableThread.__init__(self, name=name, config=config)
        self._msg_queue = incoming_message_queue

    def process_message(self, message:MessageIn):
        pass

    def run(self):
        try:
            while True:
                try:
                    #Can't wait forever in blocking call, need to enter loop to check for control messages, specifically SystemExit.
                    msg = self._msg_queue._get_message(self._get_blocking_timeout())
                    if msg:
                        _logger.info(f"Processing message: {msg}")
                        self.process_message(msg)
                except TimeoutError:
                    #This is normal when there are no more mesages in the queue and wait time has ben satisfied. Just ignore it.
                    pass
                except Exception:
                    # Log here for monitoring
                    _logger.exception(None)
                    raise

        except SystemExit:
            _logger.info(f'Shutdown signal received, exiting {self.name}')
            
        finally:
            pass

class MessageQueueDispatcher(MessageQueueWorker):
    def __init__(self, name:str, incoming_message_queue:MessageQueue, context:Context, config:dict={}):
        super().__init__(f'dhs {name} message dispatcher', incoming_message_queue, config)
        self._name = name
        self._handler_map = MessageHandlerRegistry._get_message_handlers()         
        self._context = context

    def start(self):
        super().start()
        for type, func in self._handler_map.items():
            lineno = getsourcelines(func)[1]
            module = getmodule(func)
            _logger.info(f'Registered message handler: {type}, {module.__name__}:{func.__name__}():{lineno} with {self._name} dispatcher')

    def process_message(self, message:MessageIn):
        type_id = message.get_type_id()
        handler = self._handler_map.get(type_id)
        if isfunction(handler):
            handler(message, self._context)

