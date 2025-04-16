import logging
import threading
import time

from proton.handlers import MessagingHandler
from .publisher import Publisher
import uuid


_logger = logging.getLogger(__name__)


class SyncedPublisher(Publisher):

    _replied = None
    reply_address = None
    correlation_id = None

    def __init__(self, key, address, topic=False, fqdn=False, reply_address="reply", timeout=30):
        super(Publisher, self).__init__(key, address, topic,fqdn)

        self.reply_address=address+"."+reply_address
        self.reply_topic = topic
        self.reply_fqdn = fqdn
        self._timeout = timeout


    def send_sync(self, body=None, application=None, properties=None, raw=False):

        self.correlation_id=uuid.uuid4().hex

        if properties and 'correlation_id' in properties:
            self.correlation_id = properties['correlation_id']

        self._replied=None
        def _wait_for_reply():
            super(SyncedPublisher, self).send(body=body, application=application, properties= {'correlation_id':self.correlation_id}, raw=raw)
            timeout = self._timeout
            while self._replied is None:
                time.sleep(0.05)
                timeout = timeout - 0.05

                if timeout < 0:
                    _logger.warning(f"[synced_publisher] {self._link.target.address} - timeout ({self._timeout}s)  waiting for response, consider increasing this" )
                    break


        _logger.debug(f"[synced_publisher] {self._link.target.address} - starting blocking thread" )
        t = threading.Thread(target=_wait_for_reply())
        t.start()
        t.join()

        _logger.debug(f"[synced_publisher] {self._link.target.address} - moving on {self._replied}" )
        #wait for the reply
        ret = self._replied
        self._replied = None
        return ret

    def match_correlation_id(self, correlation_id):
        return self.correlation_id is not None and correlation_id is not None and self.correlation_id == correlation_id
