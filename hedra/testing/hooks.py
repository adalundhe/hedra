import functools
import asyncio
import psutil
import httpx
from hedra.core.engines.types.sessions import (
    FastHttpSession,
    HttpSession,
    Http2Session,
    GraphQLSession,
    PlaywrightSession
)
from aiohttp import ClientSession, TCPConnector, AsyncResolver
from aiosonic.resolver import AsyncResolver as AioSonicResolver
from aiosonic import HTTPClient
from aiosonic.connectors import TCPConnector as AioSonicTCP
from gql.transport.aiohttp import AIOHTTPTransport
from easy_logger import Logger
from .test import Test


def action(name, group=None, weight=1, order=1, metadata={}, success_condition=None):
    '''
    Action Hook

    The @action(<name>, ...) decorator is required for any methods of a class
    inheriting the ActionSet class to execute as actions. All functions wrapped in the
    decorator must be asynchronous and return valid results.

    You may pass the following to a setup hook:

    - name (required:string - positional)
    - group (optional:string - keyword)
    - weight (optional:decimal - keyword)
    - order (optional:integer - keyword)
    - metadata (optional:dict - keyword - may include env, user, type, tags [list:string], and url)
    - success_condition (optional:function - keyword)

    Note that the success condition function, if provided, will be executed when results are
    assessed. The success condition function should accept a single positional argument 
    representing the response returned by the action (for example, the HTTP response object
    returned by a request made using AioHTTP and return a true/false value based on that argument.

    For example:

    @action('my_test_action', success_condition=lambda response: response.status > 200 and response.status < 400)
    def my_test_action(self):
        with self.session.get('https://www.google.com') as response:
            return await response

    '''
    def wrapper(func):
        func.action_name = name
        func.is_setup = False
        func.is_teardown = False
        func.is_action = True
        func.weight = weight
        func.order = order
        func.group = group
        func.env = metadata.get('env')
        func.user = metadata.get('user')
        func.type = metadata.get('type')
        func.tags = metadata.get('tags')
        func.url = metadata.get('url')
        func.success_condition = success_condition

        @functools.wraps(func)
        async def decorator(*args, **kwargs):
            return await func(*args, **kwargs)
                
        return decorator  
    return wrapper


def setup(name, group=None, metadata={}):
    '''
    Setup Hook

    The @setup(<name>, ...) decorator is an optional means of specifying that
    a method of a class inherting ActionSet should executor prior to testing as a means
    of setting up/initializing test state. You may specify as many setup hooks
    as needed, however all methods wrapped in the decorator must be asynchronous
    and return valid results.

    You may pass the following to a setup hook:

    - name (required:string - positional)
    - group (optional:string - keyword)
    - metadata (optional:dict - keyword - may include env, user, type, tags [list], and url)
    
    '''
    def wrapper(func):
        func.action_name = name
        func.is_setup = True
        func.is_teardown = False
        func.is_action = True
        func.weight = 0
        func.order = 0
        func.group = group
        func.method = func
        func.env = metadata.get('env')
        func.user = metadata.get('user')
        func.type = metadata.get('type')
        func.tags = metadata.get('tags')
        func.url = metadata.get('url')
        func.success_condition = None

        @functools.wraps(func)
        async def decorator(*args, **kwargs):
            return await func(*args, **kwargs)

        return decorator
    return wrapper

def teardown(name, group=None, metadata={}):
    '''
    Teardown Hook

    The @teardown(<name>, ...) decorator is an optional means of specifying that
    a method of a class inherting ActionSet should executor after testing as a means
    of cleaning up/closing any session or state created during tests. You may specify as 
    many teardown hooks as needed, however all methods wrapped in the decorator must be 
    asynchronous and return valid results.

    You may pass the following to a teardown hook:

    - name (required:string - positional)
    - group (optional:string - keyword)
    - metadata (optional:dict - keyword - may include env, user, type, tags [list], and url)
    
    '''
    def wrapper(func):
        func.action_name = name
        func.is_setup = False
        func.is_teardown = True
        func.is_action = True
        func.weight = 0
        func.order = float('inf')
        func.group = group
        func.env = metadata.get('env')
        func.user = metadata.get('user')
        func.type = metadata.get('type')
        func.tags = metadata.get('tags')
        func.url = metadata.get('url')
        func.success_condition = None

        @functools.wraps(func)
        async def decorator(*args, **kwargs):
            return await func(*args, **kwargs)

        return decorator
    return wrapper


def use(config: Test, inject=None):

    '''
    Use

    The @use(<Test>) hook offers pre-configured sessions for use with an Action Set test's built in engines.
    You must supply a reference to a valid Test class to the hook. For example:

    from hedra.testing import ActionSet, Test
    from hedra.testing.hooks import action, use


        class Config(Test):
                engine='fast-http'
                total_time='00:01:00'
                batch_size=5000
                bach_interval=0


        @use(Config)
        TestActionSet(ActionSet):

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


    from hedra.testing import ActionSet, Test
    from hedra.testing.hooks import action, use

        class Config(Test):
                engine='fast-http'
                total_time='00:01:00'
                batch_size=5000
                bach_interval=0


        @use(Config, inject=CustomSessionClass())
        TestActionSet(ActionSet):

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

        async def decorator(selected_config: Test=config):


            selected_engine = selected_config.engine_type
        
            try:
                if inject:
                    selected_engine = 'custom'
                    session = inject
                
                session = None
                pool_size = selected_config.pool_size
                connection_pool_size = 10**3 * (pool_size + 2) * 2
                dns_cache = 10**6

                if selected_engine == 'http' or selected_engine == 'websocket':

                    session = HttpSession(
                        pool_size=selected_config.pool_size,
                        dns_cache_seconds=10**8,
                        request_timeout=selected_config.request_timeout
                    )

                elif selected_engine == 'http2':

                    session = Http2Session(
                        pool_size=selected_config.pool_size,
                        request_timeout=selected_config.request_timeout
                    )

                elif selected_engine == 'fast-http' or selected_engine == 'fast-http2':
                    
                    session = FastHttpSession(
                        pool_size=selected_config.pool_size,
                        dns_cache_seconds=10**6,
                        request_timeout=selected_config.request_timeout
                    )

                elif selected_engine == 'graphql':
                    session = GraphQLSession(
                        pool_size=selected_config.pool_size,
                        dns_cache_seconds=10**8,
                        request_timeout=selected_config.request_timeout
                    )

                cls.session = session
                cls.engine_type = selected_engine

            except Exception as err:

                logger = Logger()
                session_logger = logger.generate_logger()

                session_logger.error(f'An exception occurred while assigning the built-in engine - {str(err)}')
                exit()
                
            return cls
        
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(decorator())

    return wrapper