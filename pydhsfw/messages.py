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

    def __str__(self):
        return self.get_type_id()

class MessageOut():
    
    _type_id = None

    @classmethod
    def get_type_id(cls):
        return cls._type_id

    def write(self)->bytes:
        pass

    def __str__(self):
        return self.get_type_id()
       
class MessageRegistry():

    _registry = {}

    @classmethod
    def _register_message(cls, factory_name:str, msg_cls: MessageIn):
        if cls._registry.get(factory_name) == None:
            cls._registry[factory_name] = set()
        
        cls._registry[factory_name].add(msg_cls)

    @classmethod
    def _get_factory_messages(cls, factory_name:str):
        return cls._registry.get(factory_name, set())        


def register_message(msg_type_id:str, factory_name:str=None):
    def decorator_register_message(cls):
        cls._type_id = msg_type_id
        if factory_name and issubclass(cls, MessageIn):
            MessageRegistry._register_message(factory_name, cls)
        
        return cls

    return decorator_register_message       


class MessageFactory():

    def __init__(self, name:str=None):
        self._name = name
        self._msg_map = {}
        self._register_messages()

    def _get_msg_cls(self, type_id):
        return self._msg_map.get(type_id)

    def _register_message(self, msg_cls:MessageIn):
        self._msg_map[msg_cls.get_type_id()] = msg_cls

    def _register_messages(self):
        for msg_cls in MessageRegistry._get_factory_messages(self.name):
            if issubclass(msg_cls, MessageIn):
                self._register_message(msg_cls)

    def _create_message(self, type_id, raw_msg:bytes):
       
        msg_cls = self._get_msg_cls(type_id)
        if msg_cls:
            return msg_cls.parse(raw_msg)

    def _parse_type_id(self, raw_msg:bytes)->Any:
        return NotImplemented

    @property
    def name(self):
        return self._name

    def create_message(self, raw_msg:bytes)->MessageIn:
        """Convert a raw message to a MessageIn subclass"""

        type_id = self._parse_type_id(raw_msg)
        return self._create_message(type_id, raw_msg)
    