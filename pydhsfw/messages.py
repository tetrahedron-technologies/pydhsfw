import threading 
import traceback
import functools
from collections import deque
from typing import Any
from pydhsfw.threads import AbortableThread

class MessageIn():

    _type_id = None

    @classmethod
    def get_type_id(cls):
        return cls._type_id

    @classmethod
    def parse(cls, buffer:bytes):
        pass

    @staticmethod
    def parse_type_id(cls, buffer:bytes):
        pass

class MessageOut():
    
    _type_id = None

    @classmethod
    def get_type_id(cls):
        return cls._type_id

    def write(self)->bytes:
        pass
       
class MessageRegistry():

    _registry = {}

    @classmethod
    def _register_message(cls, factory_cls_name:str, msg_cls: MessageIn):
        if cls._registry.get(factory_cls_name) == None:
            cls._registry[factory_cls_name] = set()
        
        cls._registry[factory_cls_name].add(msg_cls)

    @classmethod
    def _get_factory_messages(cls, factory_cls_name:str):
        return cls._registry.get(factory_cls_name, set())        


def register_message(msg_type_id:str, factory_cls_name:str=None):
    def decorator_register_message(cls):
        cls._type_id = msg_type_id
        if factory_cls_name and issubclass(cls, MessageIn):
            MessageRegistry._register_message(factory_cls_name, cls)
        
        return cls

    return decorator_register_message       


class MessageFactory():

    def __init__(self):
        self._msg_map = {}
        self._register_messages()

    def _get_msg_cls(self, type_id):
        return self._msg_map.get(type_id)

    def _register_message(self, msg_cls:MessageIn):
        self._msg_map[msg_cls.get_type_id()] = msg_cls

    def _register_messages(self):
        for msg_cls in MessageRegistry._get_factory_messages(self.__class__.__name__):
            if issubclass(msg_cls, MessageIn):
                self._register_message(msg_cls)

    def _create_message(self, type_id, raw_msg:bytes):
       
        msg_cls = self._get_msg_cls(type_id)
        if msg_cls:
            return msg_cls.parse(raw_msg)

    def _parse_type_id(self, raw_msg:bytes)->Any:
        return NotImplemented

    def create_message(self, raw_msg:bytes)->MessageIn:
        """Convert a raw message to a MessageIn subclass"""

        type_id = self._parse_type_id(raw_msg)
        return self._create_message(type_id, raw_msg)
    


class MessageProcessor():

    def __init__(self):
        pass
    
    def start(self):
        pass

    def process_message(self, message:MessageIn):
        pass

    def get_connection(self):
        pass 

    def stop(self):
        pass

    def _queque_message(self, message:MessageIn):
        pass

    def _set_connection(self, connection):
        pass

class MessageProcessorWorker(AbortableThread, MessageProcessor):

    def __init__(self, name):
        AbortableThread.__init__(self, name=name)
        self._deque_message = deque()
        self._deque_event = threading.Event()
        self._connection = None

    def start(self):
        AbortableThread.start(self)

    def process_message(self, message:MessageIn):
        pass

    def get_connection(self):
        return self._connection

    def stop(self):
        super().abort()
        super().join()

    def run(self):
        try:
            while True:
                try:
                    #Can't wait forever in blocking call, need to enter loop to check for control messages, specifically SystemExit.
                    msg = self._get_message(5)
                    if msg:
                        print(f'Processing message: {msg}')
                        self.process_message(msg)
                except TimeoutError:
                    #This is normal when there are no more mesages in the queue and wait time has ben statisfied. Just ignore it.
                    pass
                except Exception:
                    traceback.print_exc()
                    raise

        except SystemExit:
            self._notify_status('Connection message processor shutdown')
            
        finally:
            pass


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

    def _set_connection(self, connection):
        self._connection = connection

    def _notify_status(self, msg:str):
        print(msg)
