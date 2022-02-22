import time
import asyncio
from aiosonic.resolver import AsyncResolver
import uvloop
from zebra_async_tools.datatypes.async_list import AsyncList
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()
from aiosonic import HTTPClient
from aiosonic.connectors import TCPConnector
from aiosonic.timeout import Timeouts
from .http_engine import HttpEngine
from .utils.wrap_awaitable import async_execute_or_catch


class FastHttpEngine(HttpEngine):

    def __init__(self, config, handler):
        super(FastHttpEngine, self).__init__(
            config,
            handler
        )
        self.headers = {}
        self._timeout = None
        self._dns_cache = 10**6
        loop = asyncio.get_event_loop()
        asyncio.set_event_loop(loop)

    @classmethod
    def about(cls):
        return '''
        Fast HTTP - (fast-http)

        key-arguments:

        --request-timeout <seconds_timeout_for_individual_requests>
        
        The Fast HTTP engine is a significantly faster albeit more limited version of the HTTP engine, ideal
        for REST requests against APIs. In general, the Fast HTTP engine is 40-60 percent faster than the 
        default HTTP engine. However, the Fast HTTP engine will return errors if the request target performs 
        too slowly or is resource-intensive, and should not be used to load pages (etc.).


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
        
        timeouts = Timeouts(request_timeout=self.request_timeout)

        if self.request_timeout:
            connector = TCPConnector(
                pool_size=self._connection_pool_size, 
                ttl_dns_cache=self._dns_cache, 
                timeouts=timeouts,
                resolver=AsyncResolver()
            )

        else:
            connector = TCPConnector(
                pool_size=self._connection_pool_size, 
                ttl_dns_cache=self._dns_cache,
                resolver=AsyncResolver()
            )

        return HTTPClient(connector)

    @async_execute_or_catch()
    async def close(self):
        for teardown_action in self._teardown_actions:
            await teardown_action.execute(self.session)
        