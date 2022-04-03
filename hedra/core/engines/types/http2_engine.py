import time
import httpx
from async_tools.datatypes.async_list import AsyncList
from .http_engine import HttpEngine
from .utils.wrap_awaitable import async_execute_or_catch, wrap_awaitable_future


class Http2Engine(HttpEngine):

    def __init__(self, config, handler):
        super(Http2Engine, self).__init__(
            config,
            handler
        )

    async def yield_session(self):

        limits = httpx.Limits(
            max_keepalive_connections=self._connection_pool_size,
            max_connections=self._connection_pool_size
        )

        if self.request_timeout:
            timeout = httpx.Timeout(self.request_timeout)
            return httpx.AsyncClient(http2=True, timeout=timeout, limits=limits)
        
        else:
            return httpx.AsyncClient(http2=True, limits=limits)

    @classmethod
    def about(cls):
        return '''
        HTTP2 - (http2)
        
        key-arguments:

        --request-timeout <seconds_timeout_for_individual_requests>

        The HTTP2 engine allows for HTTP2 requests, utilizing the HTTPX asynchronous Python
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
    @async_execute_or_catch()
    async def close(self):
        for teardown_action in self._teardown_actions:
            await teardown_action.execute(self.session)

        return await self.session.aclose()