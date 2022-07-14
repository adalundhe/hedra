import functools
import inspect
from hedra.core.engines.types.common.request import Request
from hedra.core.engines.types.playwright.command import Command
from hedra.test.hooks.hook import Hook
from hedra.test.registry.registrar import registar
from typing import Dict, List, Union

from hedra.test.actions import Action
from hedra.core.engines.types.common.context import Context
from hedra.test.config import Config
from hedra.test.hooks.types import HookType
from hedra.core.engines.types.common.hooks import Hooks
from hedra.test.client import Client
from .stage import Stage



class Execute(Stage):
    name = None
    engine_type = 'http'
    config: Config = None
    context = Context()
    next_timeout = 0
    client: Client = None

    def __init__(self) -> None:
        self.actions = []
        self.hooks: Dict[str, List[Hook]] = {}
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

            method_name = method.__name__

            hook: Hook = registar.all.get(method_name)

            if hook and self.hooks.get(hook.hook_type) is None:
                self.hooks[hook.hook_type] = [hook]
            
            elif hook:
                self.hooks[hook.hook_type].append(hook)

        for hook in self.hooks.get(HookType.ACTION):
            
            selected_client = self.client[self.config.engine_type]
            selected_client.next_name = hook.name
            self.client[self.config.engine_type] = selected_client
            await hook.call(self)

            self.client.session.context.history.add_row(
                hook.name,
                batch_size=self.config.batch_size
            )

            parsed_action = self.client.session.registered.get(hook.name)

            parsed_action.hooks = Hooks(
                before=self.get_hook(parsed_action, HookType.BEFORE),
                after=self.get_hook(parsed_action, HookType.AFTER),
                before_batch=self.get_hook(parsed_action, HookType.BEFORE_BATCH),
                after_batch=self.get_hook(parsed_action, HookType.AFTER_BATCH)
            )

            hook.session = self.client.session
            hook.action = parsed_action
            

            self.actions.append(hook)

    def get_hook(self, action: Union[Request,Command], hook_type: str):
        for hook in self.hooks[hook_type]:
            if action.name in hook.names:
                return functools.partial(hook.call, self)


    async def setup(self):
        for setup_hook in self.hooks.get(HookType.SETUP):
            await setup_hook()

    async def execute(self, action: Action):
        return action

    async def teardown(self):
        for teardown_hook in self.hooks.get(HookType.TEARDOWN):

            try:
                
                await teardown_hook()

            except Exception:
                pass

        try:
            await self.client.session.close()
        
        except Exception:
            pass