from typing import AsyncIterator

from hedra.parsing.actions.types.mercury_http2_action import MercuryHTTP2Action
from .sessions import MercuryHTTP2Session
from .mercury_engine import MercuryHTTPEngine


class MercuryHTTP2Engine(MercuryHTTPEngine):

    def __init__(self, config, handler):
        super(
            MercuryHTTP2Engine,
            self
        ).__init__(config, handler)
        self.session = MercuryHTTP2Session(
            pool_size=self.config.get('batch_size', 10**3),
            request_timeout=self.config.get('request_timeout'),
            hard_cache=self.config.get('hard_cache')
        )

    async def prepare(self, actions: AsyncIterator[MercuryHTTP2Action]):
        for action in actions:
            await self.session.prepare_request(
                action.name,
                action.url,
                method=action.method,
                headers=action.headers,
                data=action.data,
                ssl=action.ssl,
                user=action.user,
                tags=action.tags
            )

    @classmethod
    def about(cls):
        return '''
        Mercury HTTP2 - (mercury-http2)

        key-arguments:

        --request-timeout <seconds_timeout_for_individual_requests>
        
        The Mercury HTTP2 engine is a prototype HTTP2 engine, ideal for REST requests against HTTP2 enbaled APIs. In 
        general, the Mercury HTTP2 engine is two to three times faster than the default HTTP2 engine. However, the 
        Mercury HTTP2 engine will return errors if the request target performs too slowly or is resource-intensive.


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
        