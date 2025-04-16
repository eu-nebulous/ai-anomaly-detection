import logging
import threading

from proton import Event
from .handler import Handler

from . import link

from proton.handlers import MessagingHandler

_logger = logging.getLogger(__name__)
_logger.setLevel(level=logging.DEBUG)


class Consumer(link.Link, MessagingHandler):
    application = None

    def __init__(self, key, address, handler: Handler, application=None, topic=False, fqdn=False):
        super(Consumer, self).__init__(key, address, topic, fqdn)
        self.application = application
        self.handler = handler
        self.handler._consumer = self

    def should_handle(self, event: Event):

        should = event.link.name == self._link.name and \
            (self.application is None or event.message.subject == self.application)

        _logger.debug(f"[{self.key}] checking if link is the same {event.link.name}={self._link.name}  "
                      f" and application {self.application}={event.message.subject}  == {should} "
                      f" and correlation_id={event.message.correlation_id}")

        return should

    def on_start(self, event: Event) -> None:
        _logger.debug(f"[{self.key}]  on_start")

    def on_message(self, event):
        _logger.debug(f"[{self.key}]  handling event with  address => {event.message.address}")
        try:
            if self.should_handle(event):
                event.delivery.settle()
                t = threading.Thread(target=self.handler.on_message, args=[self.key, event.message.address, event.message.body, event.message, self.context])
                t.start()
            else:
                event.delivery.abort()

        except Exception as e:
            _logger.error(f"Received message: {e}")
