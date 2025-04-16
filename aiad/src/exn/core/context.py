import logging

from proton.reactor import Container

from . import link
from .manager import Manager


_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

class Context:

    base = None
    handler = None
    publishers = {}
    consumers = {}
    _manager = None

    def __init__(self, base):

        self.base = base

    def start(self, manager:Manager, handler):
        self._manager = manager
        def on_ready():
            _logger.debug("[context] on_ready" )
            for key,publisher in self.publishers.items():
                self._manager.start_publisher(self,publisher)

            for key,consumer in self.consumers.items():
                self._manager.start_consumer(self,consumer)

            handler.ready(context=self)

        self._manager._on_ready=on_ready
        self._manager.start()

    def stop(self):
        if self._manager is not None and self._manager.started:
            for key,publisher in self.publishers:
                publisher._link.close()
            for key,consumer in self.consumers:
                consumer._link.close()

            self._manager.close()


    def register_publisher(self, publisher):
        if publisher.key in self.publishers:
            _logger.warning("[context] Trying to register publisher that already exists")
            return
        _logger.info(f"[context] registering publisher {publisher.key} {publisher.address}" )
        self.publishers[publisher.key] = publisher
        if self._manager is not None and self._manager.started:
            self._manager.start_publisher(self,publisher)

    def get_publisher(self, key):
        if key in self.publishers:
            return self.publishers[key]
        return None

    def has_publisher(self, key):
        return key in self.publishers

    def has_consumer(self, key):
        return key in self.consumers

    def register_consumers(self, consumer):
        if consumer.key in self.consumers:
            _logger.warning("[context] Trying to register consumer that already exists")
            return

        self.consumers[consumer.key] = consumer
        if self._manager is not None and self._manager.started:
            self._manager.start_consumer(self,consumer)

    def unregister_consumer(self, key):
        if not key in self.consumers:
            _logger.warning("[context] Trying to unregister consumer that does not exists")
            return

        consumer = self.consumers.pop(key)
        if self._manager is not None and self._manager.started:
            consumer._link.close()

    def unregister_publisher(self, key):
        if not key in self.consumers:
            _logger.warning("[context] Trying to unregister publisher that does not exists")
            return
        publisher = self.publishers.pop(key)
        if self._manager is not None and self._manager.started:
            publisher._link.close()

    def build_address_from_link(self, link: link.Link):

        if link.fqdn:
            address = link.address
            if link.topic and not link.address.startswith("topic://"):
                address = f"topic://{address}"
            return address

        address = f"{self.base}.{link.address}"
        if link.topic:
            address = f"topic://{address}"

        return address

