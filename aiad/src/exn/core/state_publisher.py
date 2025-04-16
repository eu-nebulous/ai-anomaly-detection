import datetime
import json
from enum import Enum

from proton import Message

from . import publisher

import logging

_logger = logging.getLogger(__name__)

class States(Enum):

    STARTING = "starting"
    STARTED = "started"
    READY = "ready"
    STOPPING = "stopping"
    STOPPED = "stopped"

class Publisher(publisher.Publisher):

    def __init__(self):
        super().__init__("state","state", True)

    def _send_message(self, message_type):
        self.send({"state": message_type,"message": None})

    def starting(self):
        self._send_message(States.STARTING.value)

    def started(self):
        self._send_message(States.STARTED.value)

    def ready(self):
        self._send_message(States.READY.value)

    def stopping(self):
        self._send_message(States.STOPPING.value)

    def stopped(self):
        self._send_message(States.STOPPED.value)

    def custom(self, state):
        self._send_message(state)
