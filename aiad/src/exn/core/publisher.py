import datetime
import logging

from proton import Message

from . import link

_logger = logging.getLogger(__name__)


class Publisher(link.Link):

    def send(self, body=None, application=None):
        if not body:
            body = {}

        _logger.info(f"[{self.key}] sending to {self._link.target.address} for application={application} - {body} ")
        msg = self._prepare_message(body)
        if application:
            msg.subject = application

        self._link.send(msg)

    def _prepare_message(self, body=None):

        if not body:
            body = {}

        send = {"when": datetime.datetime.utcnow().isoformat()}
        send.update(body)
        msg = Message(
            address=self._link.target.address,
            body=send
        )
        msg.content_type = "application/json"
        return msg
