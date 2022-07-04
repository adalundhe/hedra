import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()
from easy_logger import Logger
from async_tools.functions import check_event_loop

from .utils.wrap_awaitable import async_execute_or_catch, wrap_awaitable_future


class Engine:

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
        self.teardown_actions = []

    @classmethod
    def about(cls):

        engine_types = '\n\t'.join([f'- {engine}' for engine in cls.registered_engines])

        return f'''
        Engines

        key arguments:

        --engine <engine_type>

        Engines dictate "what" action we are executing (an http request, http2 request, Playwright interaction with the UI, etc.). Engines
        represent a generalized, base integration with various libraries and tooling that execute a single action of the specified engine type,
        returning the result or error. Engines also manage setup and cleanup of any underlying session logic required of the various tooling 
        integrated (a HTTP client session, etc.). All engine types contain the following methods:

        - setup (initializes a session or general test state required for test execution)

        - execute (immediately executes a single action :depreciated:)

        - defer_all (consumes all actions within a given batch and returns them as wrapped tasks for concurrent execution by the Persona)

        - close (terminates any underlying sessions or test state created for test execution)
        
        
        Currently available engines include:

        {engine_types}        
        
        For further information on each engine, run the command:

            hedra --about engine:<engine_type>


        Related Topics:

        - personas
        - batches
            
        '''

    async def create_session(self, actions=[]) -> None:
        for action in actions:
            await action()

    async def yield_session(self) -> None:
        pass

    async def defer_all(self, actions):
        async for action in actions:
            yield wrap_awaitable_future(action)

    @async_execute_or_catch()
    async def close(self):
        for teardown_action in self.teardown_actions:
            await teardown_action()
        