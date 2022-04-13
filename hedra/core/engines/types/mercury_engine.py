from typing import AsyncIterator

from hedra.parsing.actions.types.mercury_http_action import MercuryHTTPAction
from .base_engine import BaseEngine
from .sessions import MercuryHTTPSession
from .utils.wrap_awaitable import async_execute_or_catch
from mercury_http.http import MercuryHTTPClient

class MercuryHTTPEngine(BaseEngine):

    def __init__(self, config, handler):
        super(MercuryHTTPEngine, self).__init__(config, handler)
        self.session = MercuryHTTPSession(
            pool_size=self.config.get('batch_size', 10**3),
            request_timeout=self.config.get('request_timeout'),
            hard_cache=self.config.get('hard_cache')
        )

    @classmethod
    def about(cls):
        return '''
        Mercury HTTP - (mercury-http)

        key-arguments:

        --request-timeout <seconds_timeout_for_individual_requests>
        
        The Mercury HTTP engine is a prototype HTTP engine, ideal for REST requests against APIs. In general, 
        the Mercury HTTP engine is two to three times faster than the default HTTP engine. However, the Mercury HTTP 
        engine will return errors if the request target performs too slowly or is resource-intensive.


        Actions are specified as:

        - endpoint: <host_endpoint>
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - method: <rest_request_method>
        - headers: <rest_request_headers>
        - params: <rest_request_params>
        - data: <rest_request_data>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>
        
        '''

    async def yield_session(self):
        return await self.session.create()

    
    async def prepare(self, actions: AsyncIterator[MercuryHTTPAction]):
        for action in actions:
            await self.session.prepare_request(
                action.name,
                action.url,
                method=action.method,
                headers=action.headers,
                params=action.params,
                data=action.data,
                ssl=action.ssl,
                user=action.user,
                tags=action.tags
            )

    def execute(self, action):
        return action.execute(self.session)

    async def defer_all(self, actions):
        async for action in actions:
            yield action.execute(self.session)

    @async_execute_or_catch()
    async def close(self):
        for teardown_action in self._teardown_actions:
            await teardown_action.execute(self.session)
        

