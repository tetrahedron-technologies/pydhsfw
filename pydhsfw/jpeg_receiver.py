# -*- coding: utf-8 -*-
import threading
import logging
import time
import selectors
import http.server
from http import HTTPStatus
from functools import partial
from urllib.parse import urlparse
from pydhsfw.threads import AbortableThread
from pydhsfw.transport import Transport, TransportState
from typing import Any
from requests import Request
from pydhsfw.messages import (
    IncomingMessageQueue,
    OutgoingMessageQueue,
    MessageFactory,
    register_message,
)
from pydhsfw.connection import ConnectionBase, register_connection
from pydhsfw.processors import register_message_handler
from pydhsfw.http import (
    ContentType,
    FileServerRequestMessage,
    Headers,
    RequestQueue,
    RequestVerb,
    ServerMessageRequestReader,
    ServerRequestMessage,
)

_logger = logging.getLogger(__name__)


class JpegReceiverMessageRequestReader(ServerMessageRequestReader):
    def __init__(self):
        pass

    def read_request(self, request: Request) -> Request:
        if request.method == RequestVerb.POST.value:
            content_type = request.headers.get(Headers.CONTENT_TYPE.value)
            if content_type in (ContentType.JPEG.value, ContentType.PNG.value):
                request.headers[
                    Headers.DHS_REQUEST_TYPE_ID.value
                ] = 'jpeg_receiver_image_post_request'
        elif request.method == RequestVerb.GET:
            pass

        return request


class JpegReceiverRequestHandler(http.server.BaseHTTPRequestHandler):
    def __init__(self, request_queue: RequestQueue, *args, **kwargs):
        self._request_queue = request_queue
        super().__init__(*args, **kwargs)

    protocol_version = 'HTTP/1.1'
    timeout = 5

    def do_POST(self):

        data_len_hdr = self.headers.get(Headers.CONTENT_LENGTH.value)
        data_len = int(data_len_hdr)

        remainingbytes = data_len

        data = bytes()
        while remainingbytes > 0:
            chunk = self.rfile.read(remainingbytes)
            if chunk == b'':
                break
            data += chunk
            remainingbytes -= len(chunk)

        self.send_response(HTTPStatus.OK)
        self.send_header('Connection', 'close')
        self.end_headers()

        request = Request(
            method='POST', url=self.path, headers=dict(self.headers), data=data
        )
        self._request_queue.queue(request)

    def log_message(self, format: str, *args: Any) -> None:
        _logger.debug('%s - %s' % (self.address_string(), format % args))

    def log_error(self, format: str, *args: Any) -> None:
        _logger.error('%s - %s' % (self.address_string(), format % args))

    def handle_expect_100(self):
        self.log_request(HTTPStatus.CONTINUE)
        self.send_response_only(HTTPStatus.CONTINUE)
        self.end_headers()
        return True


# Need special handling for disconnecting then connecting for the HTTPServer class. Needed to derive from
# and override some methods.
if hasattr(selectors, 'PollSelector'):
    _Selector = selectors.PollSelector
else:
    _Selector = selectors.SelectSelector


class HttpAbortableServer(http.server.HTTPServer):
    def __init__(self, server_address, RequestHandlerClass, bind_and_activate=True):
        super().__init__(server_address, RequestHandlerClass, bind_and_activate)
        self._ab_is_shut_down = threading.Event()
        self._ab_shutdown_request = False

    # We would like to tell the server to shutdown, but then wait for it in another area.
    # The default implementation is to trigger the shutdown and wait for it in the same call.
    def shutdown_trigger(self):
        self._ab_shutdown_request = True

    def serve_forever(self, poll_interval=0.5):
        """Handle one request at a time until shutdown.

        Polls for shutdown every poll_interval seconds. Ignores
        self.timeout. If you need to do periodic tasks, do them in
        another thread.
        """
        self._ab_is_shut_down.clear()
        try:
            # XXX: Consider using another file descriptor or connecting to the
            # socket to wake this up instead of polling. Polling reduces our
            # responsiveness to a shutdown request and wastes cpu at all other
            # times.
            with _Selector() as selector:
                selector.register(self, selectors.EVENT_READ)

                while not self._ab_shutdown_request:
                    ready = selector.select(poll_interval)
                    # bpo-35017: shutdown() called during select(), exit immediately.
                    if self._ab_shutdown_request:
                        break
                    if ready:
                        self._handle_request_noblock()

                    self.service_actions()
        finally:
            self._ab_shutdown_request = False
            self._ab_is_shut_down.set()

    def shutdown(self):
        """Stops the serve_forever loop.

        Blocks until the loop has finished. This must be called while
        serve_forever() is running in another thread, or it will
        deadlock.
        """
        self._ab_shutdown_request = True
        self._ab_is_shut_down.wait()


class JpegReceiverTransportConnectionWorker(AbortableThread):
    def __init__(
        self,
        connection_name: str,
        url: str,
        request_queue: RequestQueue,
        config: dict = {},
    ):
        super().__init__(
            name=f'{connection_name} jpeg receiver transport connection worker',
            config=config,
        )
        self._connection_name = connection_name
        self._url = url
        self._config = config
        self._state = TransportState.DISCONNECTED
        self._desired_state = TransportState.DISCONNECTED
        self._state_change_event = threading.Event()
        self._request_queue = request_queue
        request_hander = partial(JpegReceiverRequestHandler, self._request_queue)
        self._http_server = HttpAbortableServer(
            ('', urlparse(url).port), request_hander
        )

    def connect(self):
        self._set_desired_state(TransportState.CONNECTED)

    def reconnect(self):
        self._set_desired_state(TransportState.RECONNECTED)

    def disconnect(self):
        self._set_desired_state(TransportState.DISCONNECTED)
        self._disconnect()

    def _set_desired_state(self, state: TransportState):
        if self._desired_state != state:
            self._desired_state = state
            self._state_change_event.set()

    def run(self):

        try:

            while True:
                try:
                    if self._state_change_event.wait(self._get_blocking_timeout()):
                        # Can't wait forever in blocking call, need to enter loop to check for control messages, specifically SystemExit.

                        if self._desired_state == TransportState.CONNECTED:
                            self._connect()
                        elif self._desired_state == TransportState.DISCONNECTED:
                            self._disconnect()
                        elif self._desired_state == TransportState.RECONNECTED:
                            self._reconnect()

                        self._state_change_event.clear()

                except Exception:
                    # Send all other exceptions to the log so we can analyse them to determine if
                    # they need special handling or possibly ignoring them.
                    _logger.exception(None)
                    raise

        except SystemExit:
            _logger.info(f'Shutdown signal received, exiting {self.name}')
        finally:
            try:
                self._disconnect()
            except:
                pass

    def _connect(self):
        try:
            self._http_server.serve_forever(self._get_blocking_timeout())
        except KeyboardInterrupt:
            _logger.exception(None)
            raise
        except Exception:
            _logger.exception(None)
            raise

    def _disconnect(self):
        self._http_server.shutdown_trigger()

    def _reconnect(self):
        self._http_server.shutdown()
        time.sleep(self._get_blocking_timeout())
        self._connect()


class JpegReceiverServerTransport(Transport):
    ''' Http server transport '''

    def __init__(
        self,
        connection_name: str,
        url: str,
        message_reader: JpegReceiverMessageRequestReader,
        config: dict = {},
    ):
        super().__init__(connection_name, url, config)
        self._poll_timeout = config.get(
            AbortableThread.THREAD_BLOCKING_TIMEOUT,
            AbortableThread.THREAD_BLOCKING_TIMEOUT_DEFAULT,
        )
        self._message_reader = message_reader
        self._request_queue = RequestQueue()
        self._connection_worker = JpegReceiverTransportConnectionWorker(
            connection_name, url, self._request_queue, config
        )

    def send(self, msg: Any):
        raise NotImplemented

    def receive(self) -> Request:
        try:
            request = self._request_queue.fetch(self._poll_timeout)
            if request:
                return self._message_reader.read_request(request)
        except TimeoutError:
            # Read timed out. This is normal, it just means that no messages have been sent so we can ignore it.
            pass
        except Exception:
            # Connection is lost because the socket was closed, probably from the other side.
            # Block the socket event and queue a reconnect message.
            _logger.exception(None)
            raise
            # self.reconnect()

    def connect(self):
        self._connection_worker.connect()

    def disconnect(self):
        self._connection_worker.disconnect()

    def reconnect(self):
        self._connection_worker.reconnect()

    def start(self):
        self._connection_worker.start()

    def shutdown(self):
        self.disconnect()
        self._connection_worker.abort()

    def wait(self):
        self._connection_worker.join()


@register_message('jpeg_receiver_image_post_request', 'jpeg_receiver')
class JpegReceiverImagePostRequestMessage(FileServerRequestMessage):
    def __init__(self, request):
        super().__init__(request)


class JpegReceiverServerMessageFactory(MessageFactory):
    def __init__(self):
        super().__init__('jpeg_receiver')

    def _parse_type_id(self, request: Any):
        return ServerRequestMessage.parse_type_id(request)


class JpegReceiverServerTransport(JpegReceiverServerTransport):
    def __init__(self, connection_name: str, url: str, config: dict = {}):
        super().__init__(
            connection_name, url, JpegReceiverMessageRequestReader(), config
        )


@register_connection('jpeg_receiver')
class JpegReceiverServerConnection(ConnectionBase):
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
            JpegReceiverServerTransport(connection_name, url, config),
            incoming_message_queue,
            outgoing_message_queue,
            JpegReceiverServerMessageFactory(),
            config,
        )
