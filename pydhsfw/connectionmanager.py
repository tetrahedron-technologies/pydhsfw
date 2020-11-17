# -*- coding: utf-8 -*-
import logging
from inspect import getmodule, getsourcelines
from pydhsfw.messages import OutgoingMessageQueue, IncomingMessageQueue
from pydhsfw.connection import Connection, ConnectionRegistry

_logger = logging.getLogger(__name__)


class ConnectionManager:
    def __init__(self):
        self._connections = {}
        self._connection_factory = None

    def load_registry(self):
        self._connection_factory = ConnectionFactory()

    def create_connection(
        self,
        name: str,
        scheme: str,
        url: str,
        incoming_message_queue: IncomingMessageQueue,
        outgoing_message_queue: OutgoingMessageQueue,
        config: dict = {},
    ) -> Connection:
        """Create a connection connection that has been registered.

        Create a connection that has been registered with @register_connection

        name - Name of the connection instance. This must be unique to each connection that is created. The name is used to retrieve the connection
        instance using get_connection().

        scheme - Type of connection to create. The available connection schemes are based on all connections that were registered using @register_connection

        url - Url that the connection will used when connecting to a resource.

        """
        conn = None

        if name in self._connections.keys():
            raise ValueError("Connection name already exists")

        conn = self._connection_factory.create_connection(
            name, scheme, url, incoming_message_queue, outgoing_message_queue, config
        )
        if conn:
            self._connections[name] = conn

        return conn

    def get_connection(self, name: str) -> Connection:
        return self._connections.get(name)

    def start_connections(self):
        for conn in self._connections.values():
            conn.start()

    def shutdown_connections(self):
        for conn in self._connections.values():
            conn.shutdown()

    def wait_connections(self):
        for conn in self._connections.values():
            conn.wait()


class ConnectionFactory:
    def __init__(self):
        self._registry = ConnectionRegistry._get_connection_classes()
        for scheme, conn_cls in self._registry.items():
            lineno = getsourcelines(conn_cls)[1]
            module = getmodule(conn_cls)
            _logger.info(
                f"Registered connection class: {scheme}, {module.__name__}:{conn_cls.__name__}():{lineno} with connection registry"
            )

    def create_connection(
        self,
        connection_name: str,
        scheme: str,
        url: str,
        incoming_message_queue: IncomingMessageQueue,
        outgoing_message_queue: OutgoingMessageQueue,
        config: dict = None,
    ) -> Connection:

        connection = None
        conn_cls = self._registry.get(scheme)
        if conn_cls:
            connection = conn_cls(
                connection_name,
                url,
                incoming_message_queue,
                outgoing_message_queue,
                config,
            )

        return connection
