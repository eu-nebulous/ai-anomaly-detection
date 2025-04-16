import datetime
import logging

from proton import Message,AnnotationDict

from . import link

_logger = logging.getLogger(__name__)


class Publisher(link.Link):

    def send(self, body=None, application=None, properties=None, raw=False):
        if not body:
            body = {}

        _logger.info(f"[{self.key}] sending to {self._link.target.address} for application={application} - {body} "
                     f" properties= {properties}")

        msg = self._prepare_message(body,properties=properties, raw=raw)

        if application:
            msg.subject = application
            msg.properties={
                'application': application
            }

        self._link.send(msg)

    def _prepare_message(self, body=None,  properties=None, raw=False):

        send = {}

        if not body:
            body = {}

        if not raw:
            send = {"when": datetime.datetime.utcnow().isoformat()}

        send.update(body)
        msg = Message(
            address=self._link.target.address,
            body=send
        )

        if properties:
            if 'correlation_id' in properties:
                msg.correlation_id=properties['correlation_id']

        msg.content_type = 'application/json'

        return msg
