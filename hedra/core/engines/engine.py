from .types import (
    HttpEngine,
    Http2Engine,
    ActionSetEngine,
    FastHttpEngine,
    FastHttp2Engine,
    PlaywrightEngine,
    WebsocketEngine,
    GrpcEngine,
    GraphQLEngine
)
from easy_logger import Logger


class Engine:

    registered_engines = {
        'http': HttpEngine,
        'http2': Http2Engine,
        'fast-http': FastHttpEngine,
        'fast-http2': FastHttp2Engine,
        'action-set': ActionSetEngine,
        'playwright': PlaywrightEngine,
        'websocket': WebsocketEngine,
        'grpc': GrpcEngine,
        'graphql': GraphQLEngine
    }

    def __init__(self, config, handler):
        logger = Logger()
        self.session_logger = logger.generate_logger()
        self.type = config.get('engine_type', 'http')

        self._event_loop = None
        self.engine = None
        self.config = config
        self.handler = handler

        self.engine = self.registered_engines.get(
            self.type,
            HttpEngine
        )(
            self.config,
            self.handler
        )

        self.max_connections = self.engine._connection_pool_size

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

    async def setup(self, actions):
        await self.engine.create_session(actions)
        
    async def defer_all(self, actions):
        async for response in self.engine.defer_all(actions):
            yield response

    async def close(self):
        return await self.engine.close()
        