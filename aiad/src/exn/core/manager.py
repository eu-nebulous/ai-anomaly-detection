import logging

from proton import Event, Connection,Session

from proton.handlers import MessagingHandler
from proton.reactor import Container

from .consumer import Consumer
from .publisher import Publisher

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)


class SessionPerConsumer(object):
    def session(self, connection: Connection) -> Session:
        session = connection.session()
        session.open()
        return session


class Manager(MessagingHandler):
    uri = None
    started = False
    container = None
    connection = None

    _on_ready = None

    def __init__(self, uri):
        super(Manager, self).__init__()
        self.uri = uri

    def start(self):
        _logger.info(f"[manager] starting")
        self.container = Container(self)
        self.container.run()

    def on_start(self, event: Event) -> None:
        self.connection = self.container.connect(self.uri)
        self.connection._session_policy=SessionPerConsumer()

        self.started=True
        _logger.debug(f"[manager] on_start")
        if self._on_ready is not None:
            self._on_ready()

    def on_message(self, event: Event) -> None:
        _logger.warning(f"[manager] received generic on_message make sure you have set up your handlers"
                        f" properly ")

    def close(self):
        _logger.info(f"[manager] closing")
        if self.container:
            self.container.stop()

        if self.connection:
            self.connection.close()

    def start_publisher(self, context, publisher: Publisher):
        address = context.build_address_from_link(publisher)
        _logger.info(f"[manager] starting publisher {publisher.key} => {address}")
        publisher.set(self.container.create_sender(self.connection, address))
        if hasattr(publisher, "delay"):
            _logger.debug(f"{context.base} registering timer {hasattr(publisher, 'delay')}")
            self.container.schedule(publisher.delay, handler=publisher)

    def start_consumer(self, context, consumer: Consumer):
        address = context.build_address_from_link(consumer)
        _logger.info(f"[manager] starting consumer {consumer.key} => {address}")
        consumer.set(self.container.create_receiver(self.connection, address , handler=consumer))
