# -*- coding: utf-8 -*-
from threading import Event
from collections import deque
from typing import Any, TypeVar, Generic


class MessageIn:

    _type_id = None

    @classmethod
    def get_type_id(cls):
        return cls._type_id

    @classmethod
    def parse(cls, buffer: Any):
        pass

    @staticmethod
    def parse_type_id(cls, buffer: Any) -> Any:
        pass

    def __str__(self):
        return self.get_type_id()


class MessageOut:

    _type_id = None

    @classmethod
    def get_type_id(cls):
        return cls._type_id

    def write(self) -> Any:
        pass

    def __str__(self):
        return self.get_type_id()


class MessageRegistry:

    _registry = {}

    @classmethod
    def _register_message(cls, factory_name: str, msg_cls: MessageIn):
        if cls._registry.get(factory_name) is None:
            cls._registry[factory_name] = set()

        cls._registry[factory_name].add(msg_cls)

    @classmethod
    def _get_factory_messages(cls, factory_name: str):
        return cls._registry.get(factory_name, set())


def register_message(msg_type_id: str, factory_name: str = None):
    """Registers a MessageIn class with a message factory and assigns it a message type id..

    msg_type_id - The message type id that uniquely identifies this message class. This decorator adds the message
    type id to the class definition.

    factory_name - The name of the message factory that will convert this message type id's raw messages in to the message instances.

    """

    def decorator_register_message(cls):
        cls._type_id = msg_type_id
        if factory_name and issubclass(cls, MessageIn):
            MessageRegistry._register_message(factory_name, cls)

        return cls

    return decorator_register_message


class MessageFactory:
    def __init__(self, name: str = None):
        self._name = name
        self._msg_map = {}
        self._register_messages()

    def _get_msg_cls(self, type_id):
        return self._msg_map.get(type_id)

    def _register_message(self, msg_cls: MessageIn):
        self._msg_map[msg_cls.get_type_id()] = msg_cls

    def _register_messages(self):
        for msg_cls in MessageRegistry._get_factory_messages(self.name):
            if issubclass(msg_cls, MessageIn):
                self._register_message(msg_cls)

    def _create_message(self, type_id, raw_msg: Any):

        msg_cls = self._get_msg_cls(type_id)
        if msg_cls:
            return msg_cls.parse(raw_msg)

    def _parse_type_id(self, raw_msg: Any) -> Any:
        return NotImplemented

    @property
    def name(self):
        return self._name

    def create_message(self, raw_msg: bytes) -> MessageIn:
        """Convert a raw message to a MessageIn subclass"""

        type_id = self._parse_type_id(raw_msg)
        return self._create_message(type_id, raw_msg)


T = TypeVar("T")


class Queue(Generic[T]):
    def __init__(self):
        pass

    def queue(self, item: T):
        pass

    def fetch(self, timeout=None) -> T:
        pass

    def clear(self):
        pass


class BlockingQueue(Queue[T]):
    def __init__(self):
        super().__init__()
        self._deque = deque()
        self._deque_event = Event()

    def queue(self, item: T):
        # Append message and unblock
        self._deque.append(item)
        self._deque_event.set()

    def fetch(self, timeout=None) -> T:

        item = None

        # Block until items are available
        if not self._deque_event.wait(timeout):
            raise TimeoutError

        elif self._deque:
            item = self._deque.popleft()

        # If there are no more items, start blocking again
        if not self._deque:
            self._deque_event.clear()
        return item

    def clear(self):
        self._deque_event.clear()
        self._deque.clear()


class IncomingMessageQueue(BlockingQueue[MessageIn]):
    def __init__(self):
        super().__init__()


class OutgoingMessageQueue(BlockingQueue[MessageOut]):
    def __init__(self):
        super().__init__()
