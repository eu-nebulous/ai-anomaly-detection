import logging
import os

from proton.reactor import Container

from exn.core import state_publisher, schedule_publisher
from exn.core.context import Context
from .core.manager import Manager
from .settings import base
from .handler import connector_handler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
_logger = logging.getLogger(__name__)


class EXN:

    context = None
    container = None

    def __init__(self, component=None,
                 handler:connector_handler.ConnectorHandler = None,
                 publishers=None,
                 consumers=None,
                **kwargs):

        # Load .env file
        # Validate and set connector
        if not component:
            _logger.error("Component cannot be empty or None")
            raise ValueError("Component cannot be empty or None")
        self.component = component

        self.url = kwargs.get('url',os.getenv('NEBULOUS_BROKER_URL'))
        self.port = kwargs.get('port', os.getenv('NEBULOUS_BROKER_PORT'))
        self.username = kwargs.get('username',os.getenv('NEBULOUS_BROKER_USERNAME'))
        self.password = kwargs.get('password', os.getenv('NEBULOUS_BROKER_PASSWORD'))
        self.handler = handler

        # Validate attributes
        if not self.url:
            _logger.error("URL cannot be empty or None")
            raise ValueError("URL cannot be empty or None")
        if not self.port:
            _logger.error("PORT cannot be empty or None")
            raise ValueError("PORT cannot be empty or None")
        if not self.username:
            _logger.error("USERNAME cannot be empty or None")
            raise ValueError("USERNAME cannot be empty or None")
        if not self.password:
            _logger.error("PASSWORD cannot be empty or None")
            raise ValueError("PASSWORD cannot be empty or None")

        self.context = Context(base=f"{base.NEBULOUS_BASE_NAME}.{self.component}")

        if not publishers:
            publishers = []

        if not consumers:
            consumers = []

        compiled_publishers = publishers
        if kwargs.get("enable_state",False):
            compiled_publishers.append(state_publisher.Publisher())

        if kwargs.get("enable_health",False):
            compiled_publishers.append(schedule_publisher.Publisher(
                base.NEBULOUS_DEFAULT_HEALTH_CHECK_TIMEOUT,
                'health',
                'health',
                topic=True))

        for c in consumers:
            self.context.register_consumers(c)

        for p in compiled_publishers:
            self.context.register_publisher(p)

    def start(self):
        self.context.start(Manager(f"{self.url}:{self.port}", self.username,self.password),self.handler)


    def stop(self):
        self.context.stop()
