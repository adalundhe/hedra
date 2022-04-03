import time
import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
uvloop.install()
from .fast_http_engine import FastHttpEngine
from .utils.wrap_awaitable import async_execute_or_catch, wrap_awaitable_future


class FastHttp2Engine(FastHttpEngine):

    def __init__(self, config, handler, actions=None):
        super(FastHttp2Engine, self).__init__(
            config,
            handler,
            actions
        )
        self.headers = {}
        self._timeout = None
        self._dns_cache = 10**6

    @classmethod
    def about(cls):
        return '''
        Fast HTTP - (fast-http)

        key-arguments:

        --request-timeout <seconds_timeout_for_individual_requests>

        The Fast HTTP2 engine is a significantly faster albeit more limited version of the HTTP2 engine, ideal
        for REST requests against APIs. In general, the Fast HTTP2 engine is 40-60 percent faster than the 
        default HTTP2 engine. However, the Fast HTTP2 engine will return errors if the request target performs 
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