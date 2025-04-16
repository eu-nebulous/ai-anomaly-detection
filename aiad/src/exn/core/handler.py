import logging

from proton import Message

_logger = logging.getLogger(__name__)


class Handler:

    def on_message(self, key, address, body, message: Message, context):
        _logger.info(f"You should really override this... {key}=>{address}")
