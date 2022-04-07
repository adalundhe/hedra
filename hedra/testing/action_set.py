from telnetlib import EXOPL
from hedra.core.engines import Engine
from hedra.parsing.actions.action import Action
from async_tools.datatypes.async_list import AsyncList

from hedra.parsing.actions.types.http_action import HttpAction
from .hooks import setup, teardown
from hedra.parsing.actions import Action
from .hooks import (
    setup,
    teardown
)


class ActionSet:
    name=None
    engine_type='http'
    session=None
    fixtures={}
    config={}

    def __init__(self) -> None:
        engine = Engine({
            'engine_type': self.engine_type,
            **self.config
        }, None)
        self.engine = engine.engine
        self.actions = AsyncList()
        self.action_type = Action.action_types.get(self.engine_type, HttpAction)
        self.fixtures = self.fixtures

    @classmethod
    def about(cls):
        return '''
        Action Set

        An Action Set must be inherited by a Python class in test code in order for that
        class's methods (decorated by the required setup, action, or teardown hooks) to 
        be used as actions.
        
        The Action Set provides convience setup and teardown actions (not executed as a part of testing) 
        to manage built-in Engine setup/teardown if you wish to use one of Hedra's built in engines. The 
        Action Set class also provides a convenience execute() method that may be used in tandem with the 
        engine type specified via the engine class attribute to pass action data to the specified engine. 
        For example:


        @action('my_test_action', order=1)
        def my_test_action(self):
            return await self.execute(action_data_dict, group='group1')


        Note that if you choose to utilize the built-in engines, you should supply a matcching session
        to the *session* Action Set class attribute:


            from aiohttp import ClientSession, TCPConnector

            TestActionSet(ActionSet):
                engine='http'
                session=ClientSession(TCPConnector(limit=10**6, ttl_dns_lookup=10**6))


        combined this appears as:


            from hedra.testing import ActionSet
            from hera.testing.hooks import action
            from aiohttp import ClientSession, TCPConnector

            TestActionSet(ActionSet):
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

    @setup('setup_action_set')
    async def setup(self):
        self.engine.session = await self.session.create()

    def execute(self, action_data: dict, group: str=None):
        return self.engine.execute(
            self.action_type(
                action_data,
                group=group
            )
        )

    @teardown('teardown_action_set')
    async def close(self):
        try:
            await self.engine.close()
        except Exception:
            pass
    