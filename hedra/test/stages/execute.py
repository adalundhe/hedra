import ast
import inspect
import importlib
import gc
from typing import Union
from hedra.core.engines.types import (
    MercuryGraphQLClient,
    MercuryGRPCClient,
    MercuryHTTP2Client,
    MercuryHTTPClient,
    MercuryPlaywrightClient,
    MercuryWebsocketClient
)

from hedra.test.actions import Action
from hedra.test.actions.http2 import HTTP2Action
from hedra.test.hooks import teardown
from hedra.core.engines.types.common.context import Context
from hedra.test.config import Config
from hedra.test.hooks.types import HookType
from hedra.test.registry import registered, Registry
from .stage import Stage


MercuryEngine = Union[MercuryGraphQLClient, MercuryGRPCClient, MercuryHTTP2Client, MercuryHTTPClient, MercuryPlaywrightClient, MercuryWebsocketClient]


class Execute(Stage):
    name = None
    engine_type = 'http'
    session: MercuryEngine = None
    config: Config = None
    actions = []
    setup_actions = []
    teardown_actions = []
    context = Context()
    next_timeout = 0

    def __init__(self) -> None:
        self.session = self.session
        self.actions = self.actions
        self.hooks = {}
        self.registry = Registry()
        self.name = type(self).__name__

        for hook_type in HookType:
            self.hooks[hook_type] = []

    @classmethod
    def about(cls):
        return '''
        Execute Stage

        An Execute stage must be inherited by a Python class in test code in order for that
        class's methods (decorated by the required setup, action, or teardown hooks) to 
        be used as actions.
        
        The Execute stage provides convience setup and teardown actions (not executed as a part of testing) 
        to manage built-in Engine setup/teardown if you wish to use one of Hedra's built in engines. The 
        Execute stage class also provides a convenience execute() method that may be used in tandem with the 
        engine type specified via the engine class attribute to pass action data to the specified engine. 
        For example:


        @action('my_test_action', order=1)
        def my_test_action(self):
            return await self.execute(action_data_dict, group='group1')


        Note that if you choose to utilize the built-in engines, you should supply a matcching session
        to the *session* Execute stage class attribute:


            from aiohttp import ClientSession, TCPConnector

            TestActionSet(ActionSet):
                engine='http'
                session=ClientSession(TCPConnector(limit=10**6, ttl_dns_lookup=10**6))


        combined this appears as:


            from hedra.test import Execute
            from hera.test.hooks import action
            from aiohttp import ClientSession, TCPConnector

            MyExecuteStage(Execute):
                engine='http'
                session=ClientSession(TCPConnector(limit=10**6, ttl_dns_lookup=10**6))

                @action('test_httpbin_get')
                def test_httpbin_get(self):
                    return await self.execute({
                        'method': 'GET',
                        'endpoint': '/get',
                        'host': 'https://httpbin.org'
                    })

        '''

    @classmethod
    def about_hooks(cls):
        return '''
        Hooks

        Hooks are pure-Python decorator functions that, when used to decorate methods of a class 
        inheriting the ActionSet class, allow Hedra to utilize Python code to specify actions.
        Supported hooks include:

        - use
        - setup
        - action
        - teardown

        For more information on a specific hook run the command:

            hedra --about hooks:<hook_type>        
        '''
   
    async def register_actions(self):
        methods = inspect.getmembers(self, predicate=inspect.ismethod) 

        for _, method in methods:
                if hasattr(method, 'is_action'):
                    self.hooks[method.hook_type].append(method)
                    if method.hook_type == HookType.ACTION:
                        action: Action = await method()
                        action.to_type(method.name)
                        
                        action.order = method.order
                        action.weight = method.weight

                        result = await self.session.prepare(action.parsed, action.checks)
                        if result and result.error:
                            raise result.error

                        self.session.context.history.add_row(
                            action.parsed.name,
                            batch_size=self.config.batch_size
                        )

                        action.session = self.session

                        self.registry[method.name] = action

    async def setup(self):
        for setup_hook in self.hooks.get(HookType.SETUP):
            await setup_hook()

    async def execute(self, action: Action):
        return action

    async def teardown(self):
        for teardown_hook in self.hooks.get(HookType.TEARDOWN):
            await teardown_hook()

        await self.session.close()