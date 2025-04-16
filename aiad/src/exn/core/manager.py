import logging
import threading
import time
import types

import proton
from proton import Event, Connection, Session, Message

from proton.handlers import MessagingHandler
from proton.reactor import Container,ReceiverOption,Filter,Selector

from .consumer import Consumer
from .publisher import Publisher
from .handler import Handler
from .synced_publisher import SyncedPublisher

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

    def __init__(self, uri, username=None, password=None):
        super(Manager, self).__init__()
        self.uri = uri
        self.username = username
        self.password = password

    def start(self):
        _logger.info(f"[manager] starting")
        self.container = Container(self)
        self.container.run()

    def on_start(self, event: Event) -> None:
        if self.username:
            # basic username and password authentication
            self.connection = event.container.connect(url=self.uri,
                                                      user=self.username,
                                                      password=self.password,
                                                      allow_insecure_mechs=True)
        else:
            # Anonymous authentication
            self.connection = event.container.connect(url=self.uri)

        self.connection._session_policy = SessionPerConsumer()

        def connection_state():
            while self.connection.state != 18:
                time.sleep(0.05)
            self.started = True
            _logger.debug(f"[manager] on_start")
            if self._on_ready is not None:
                self._on_ready()

        threading.Thread(target=connection_state).start()

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
        publisher.context = context
        if hasattr(publisher, "delay"):
            _logger.debug(f"{context.base} registering timer {hasattr(publisher, 'delay')}")
            self.container.schedule(publisher.delay, handler=publisher)

        if hasattr(publisher, "reply_address"):
            _logger.info(f"[manager] starting Synced consumer for  {publisher.key} => {publisher.reply_address}")
            def on_my_message(self, key, address, body, message: Message, context=None):
                _logger.info(f"[manager]  [{publisher.key}] handler received  {key} => {message.correlation_id}")
                if publisher.match_correlation_id(message.correlation_id):
                    _logger.info(f"[manager]  [{publisher.key}] handler received  {key} => {message.correlation_id} matched => {body}")
                    publisher._replied = body
                else:
                    _logger.warning(f"[manager]  [{publisher.key}] handler received  {key} => {message.correlation_id} *NOT MATCHED*")

            r_handler = Handler()
            r_handler.on_message= types.MethodType(on_my_message,r_handler)
            self.start_consumer(
                context,
                Consumer(publisher.key+"-reply",
                         publisher.reply_address,
                         handler=r_handler,
                         topic=publisher.reply_topic,
                         fqdn=publisher.reply_fqdn
                         )
            )

    def start_consumer(self, context, consumer: Consumer):
        address = context.build_address_from_link(consumer)
        consumer.context = context
        if consumer.application:
            _logger.info(f"[manager] starting consumer {consumer.key} => {address} and application={consumer.application}")
            consumer.set(self.container.create_receiver(
                self.connection,
                address,
                handler=consumer,
                options=Selector(u"application = '"+consumer.application+"'"))
            )

        else:
            _logger.info(f"[manager] starting consumer {consumer.key} => {address}")
            consumer.set(self.container.create_receiver(self.connection, address, handler=consumer))


