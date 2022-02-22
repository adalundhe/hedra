from zebra_async_tools.datatypes.async_list import AsyncList
from .base_engine import BaseEngine
from .utils.wrap_awaitable import async_execute_or_catch, wrap_awaitable_future


class ActionSetEngine(BaseEngine):

    def __init__(self, config, handler):
        super(ActionSetEngine, self).__init__(
            config,
            handler
        )

    @classmethod
    def about(cls):
        return '''
        
        Action Set - (action-set)

        The Action Set engine allows for the execution of Python script tests and must be selected if executing
        Python code actions. The Action Set engine consumes a single valid Python function and executes that function, 
        providing timing measurement and error/exception catching.

        Actions are specified as Python class methods of any valid class inheriting the ActionSet class from Hedra's
        Testing package. Methods must also be wrapped by one of the following decorators:

        - @setup(<name>, *args, **kwargs)
        - @action(<name>, *args, **kwargs)
        - @teardown(<name>, *args, **kwargs)

        For more information on Hedra's Testing package, writing tests as Python code, hooks, or the ActionSet class
        run the command:

            hedra --about testing

        '''

    async def create_session(self, actions=AsyncList()) -> None:
        for action in actions.data:
            if action.is_setup:
                await action.execute(action)
            elif action.is_teardown:
                self._teardown_actions.append(action)
        

    async def yield_session(self) -> None:
        pass

    async def defer_all(self, actions):
        async for action in actions:
            yield wrap_awaitable_future(
                action,
                action
            )

    @async_execute_or_catch()
    async def close(self):
        for teardown_action in self._teardown_actions:
            await teardown_action.execute(teardown_action)