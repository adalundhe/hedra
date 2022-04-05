import psutil
from async_tools.datatypes.async_list import AsyncList
from .sessions import HttpSession
from .base_engine import BaseEngine
from .utils.wrap_awaitable import async_execute_or_catch, wrap_awaitable_future


class HttpEngine(BaseEngine):

    def __init__(self, config, handler):
        super(HttpEngine, self).__init__(
            config,
            handler
        )

        self.session = HttpSession(
            pool_size=self._pool_size,
            dns_cache_seconds=10**8,
            request_timeout=self.config.get('request_timeout')
        )

    @classmethod
    def about(cls):
        return '''
        HTTP - (http)

        key-arguments:

        --request-timeout <seconds_timeout_for_individual_requests>

        The HTTP engine is the default engine for Hedra, utilizing the AioHttp asynchronous Python
        library. It is both fast and robust, allowing for requests to both REST APIs and non-traditional
        usage like refreshing/re-loading pages.

        Actions are specified as:

        - endpoint: <host_endpoint>
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - method: <rest_request_method>
        - headers: <rest_request_headers>
        - auth: <rest_request_auth>
        - params: <rest_request_params>
        - data: <rest_request_data>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>
        
        '''

    async def create_session(self, actions=AsyncList()):
        for action in actions.data:
            if action.is_setup:
                await action.execute(self.session)
            elif action.is_teardown:
                self._teardown_actions.append(action)

        self.session = await self.yield_session()

    async def yield_session(self):
        return await self.session.create()

    def execute(self, action):
        return action.execute(self.session)

    async def defer_all(self, actions):
        async for action in actions:
            yield wrap_awaitable_future(
                action,
                self.session
            )

    @async_execute_or_catch()
    async def close(self):
        for teardown_action in self._teardown_actions:
            await teardown_action.execute(self.session)

        await self.session.close()