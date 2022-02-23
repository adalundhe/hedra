import asyncio
from easy_logger import Logger
from .bootstrap_services import BootstrapManager


class BootstrapServer:

    def __init__(self, config) -> None:
        logger = Logger()
        self.session_logger = logger.generate_logger()
        self.manager = BootstrapManager(config)
        self.loop = None

    def start(self):
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(self.manager.start_server())

    def run(self):
        self.loop.run_until_complete(self.manager.wait())

    def kill(self):
        self.loop.run_until_complete(self.manager.stop())