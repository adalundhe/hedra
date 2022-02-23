import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()
from easy_logger import Logger
from async_tools.functions import check_event_loop


class BaseEngine:

    def __init__(self, config, handler):
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self.handler = handler
        self.config = config

        check_event_loop(self.session_logger)
        self._event_loop = asyncio.get_event_loop()
        self.session = None
        self._pool_size = self.config.get('pool_size', 1)

        self._connection_pool_size = 10**5 * (self._pool_size + 2) * 2
        self._setup_action = None
        self._teardown_actions = []