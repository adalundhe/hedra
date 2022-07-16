import asyncio
from hedra.core.engines.types.playwright import ContextConfig
from hedra.core.engines.types import (
    MercuryGraphQLClient,
    MercuryHTTPClient,
    MercuryHTTP2Client,
    MercuryWebsocketClient,
    MercuryGRPCClient,
    MercuryPlaywrightClient
)
from hedra.core.engines.types.common import Timeouts
from easy_logger import Logger
from hedra.core.personas.utils.time import parse_time
from hedra.test.config import Config
from hedra.test.client import Client









def configure(config: Config):

    '''
    Use

    The @use(<Config>) hook offers pre-configured sessions for use with an Action Set test's built in engines.
    You must supply a reference to a valid Config class to the hook. For example:

    from hedra.test import Execute, Config
    from hedra.test.hooks import action, use


        class MyConfig(Config):
                engine='fast-http'
                total_time='00:01:00'
                batch_size=5000
                bach_interval=0


        @use(MyConfig)
        MyExecuteStage(Execute):

            @action('test_httpbin_get')
            async def test_httpbin_get(self):
                return await self.execute({
                    'endpoint': '/get',
                    'host': 'https://httpbin.org',
                    'method': 'GET'
                })

    Currently supported engines include:

    - http
    - http2
    - fast-http
    - fast-http2
    - graphql

    The default is http. If you wish to supply your own session in can be passed via the "inject" 
    keyword argument:


    from hedra.test import Execute, Config
    from hedra.test.hooks import action, use

        class MyConfig(Config):
                engine='fast-http'
                total_time='00:01:00'
                batch_size=5000
                bach_interval=0


        @use(Config, inject=CustomSessionClass())
        MyExecuteStage(Execute):

            @action('test_httpbin_get')
            async def test_httpbin_get(self):
                return await self.execute({
                    'endpoint': '/get',
                    'host': 'https://httpbin.org',
                    'method': 'GET'
                })

    The Use hook is ideal for reducing boilerplate code and providing a robust, high-concurrency base upon
    which to build your tests. 

    '''

    def wrapper(cls):

        async def decorator(selected_config: Config=config):


            selected_engine = selected_config.engine_type
            pool_size = 1
            pool_concurrency = selected_config.options.get('session_max_connections')

            if selected_config.runner_mode == 'parallel' and pool_concurrency is None:
                pool_size = selected_config.pool_size
                pool_concurrency = int(selected_config.batch_size/pool_size)  

            elif pool_concurrency is None:
                pool_concurrency = selected_config.batch_size   

            total_time = parse_time(selected_config.total_time)
            if selected_config.request_timeout > total_time:
                selected_config.request_timeout = total_time         

            try:
                
                session = None

                if selected_engine == 'http':

                    session = MercuryHTTPClient(
                        concurrency=pool_concurrency,
                        timeouts=Timeouts(
                            total_timeout=selected_config.request_timeout
                        ),
                        hard_cache=selected_config.options.get('hard_cache', False),
                        reset_connections=selected_config.options.get('reset_connections')
                    )

                elif selected_engine == 'http2':
                    session = MercuryHTTP2Client(
                        concurrency=pool_concurrency,
                        timeouts=Timeouts(
                            connect_timeout=selected_config.connect_timeout,
                            total_timeout=selected_config.request_timeout
                        ),
                        hard_cache=selected_config.options.get('hard_cache', False),
                        reset_connections=selected_config.options.get('reset_connections')
                    )

                elif selected_engine == 'graphql':
                    session = MercuryGraphQLClient(
                        concurrency=pool_concurrency,
                        timeouts=Timeouts(
                            total_timeout=selected_config.request_timeout
                        ),
                        hard_cache=selected_config.options.get('hard_cache', False),
                        reset_connections=selected_config.options.get('reset_connections')
                    )

                    session.protocol.context = cls.context

                elif selected_engine == 'websocekt':
                    session = MercuryWebsocketClient(
                        concurrency=pool_concurrency,
                        timeouts=Timeouts(
                            total_timeout=selected_config.request_timeout
                        ),
                        hard_cache=selected_config.options.get('hard_cache', False),
                        reset_connections=selected_config.options.get('reset_connections')
                    )

                elif selected_engine == 'grpc':
                    session = MercuryGRPCClient(
                        concurrency=pool_concurrency,
                        timeouts=Timeouts(
                            total_timeout=selected_config.request_timeout
                        ),
                        hard_cache=selected_config.options.get('hard_cache', False),
                        reset_connections=selected_config.options.get('reset_connections')
                    )

                elif selected_engine == 'playwright':
                    context_config=selected_config.options.get('session_playwright_context', ContextConfig())
                    
                    session = MercuryPlaywrightClient(
                        concurrency=pool_concurrency,
                        group_size=selected_config.options.get('session_group_size', 25),
                        timeouts=Timeouts(
                            total_timeout=selected_config.request_timeout
                        )
                    )

                    await session.setup(context_config)

                # cls.context.history.row_size = selected_config.batch_size

                # session.context = cls.context
                cls.client = Client(session)
                cls.config = selected_config

            except Exception as err:

                logger = Logger()
                session_logger = logger.generate_logger()

                session_logger.error(f'An exception occurred while assigning the built-in engine - {str(err)}')
                exit()
                
            return cls
        
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(decorator())

    return wrapper