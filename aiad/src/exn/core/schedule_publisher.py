import logging

from proton.handlers import MessagingHandler
from .publisher import Publisher

_logger = logging.getLogger(__name__)


class Publisher(Publisher, MessagingHandler):
    send_next = False
    delay = 15

    def __init__(self, delay, key, address, application=None, topic=False, fqdn=False):
        super(Publisher, self).__init__(key, address, topic,fqdn)
        self.delay = delay
        self.application = application

    def on_timer_task(self, event):
        _logger.debug(f"[manager] on_timer_task")
        self.send()
        event.reactor.schedule(self.delay, self)

    def send(self, body=None, application=None):
        super(Publisher, self).send(body, self.application)
