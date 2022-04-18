from typing import AsyncIterator
from hedra.parsing.actions.types.mercury_grpc_action import MercuryGRPCAction
from .mercury_http_engine import MercuryHTTPEngine
from .sessions import MercuryGRPCSession


class MercuryGRPCEngine(MercuryHTTPEngine):

    def __init__(self, config, handler):
        super(
            MercuryGRPCEngine,
            self
        ).__init__(config, handler)

        self.session = MercuryGRPCSession(
            pool_size=self.config.get('batch_size', 10**3),
            request_timeout=self.config.get('request_timeout'),
            hard_cache=self.config.get('hard_cache'),
            reset_connections=self.config.get('reset_connections')
        )

    
    async def prepare(self, actions: AsyncIterator[MercuryGRPCAction]):
        for action in actions:
            await self.session.prepare_request(action.request)

    @classmethod
    def about(cls):
        return '''
        Mercury GRPC Engine - (grpc)

        The Mercury GRPC Engine executes GRPC calls against any GRPC service. These calls may be single 
        unary operations, request streams, response streams, or bidirectional streams.

        Actions are specified as:

        - endpoint: <host_endpoint>
        - url: <full_url_to_target>
        - headers: <rest_request_headers>
        - data: <grpc_protobuf_object>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        '''

        