import time

from zebra_async_tools.datatypes.async_list import AsyncList
from .base_engine import BaseEngine
from .utils.wrap_awaitable import async_execute_or_catch, wrap_awaitable_future


class GrpcEngine(BaseEngine):

    def __init__(self, config, handler):
        super().__init__(config, handler)

    @classmethod
    def about(cls):
        return '''
        GRPC Engine - (grpc)

        The GRPC Engine executes GRPC calls against any GRPC service. These calls may be single 
        unary operations, request streams, response streams, or bidirectional streams.

        Actions are specified as:

        - proto_path: <path_to_proto_file_to_use> (i.e. protos/test.proto)
        - proto_name: <name_of_rpc_type_to_use>
        - endpoint: <name_of_rpc_function_to_use>
        - data: <data_to_pass_to_rpc_type>
        - streaming_type: <type_of_streaming> (should be - 'response', 'request', or 'bidirectional')
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - method: <method_associated_with_graphql_query_type> (should be GET or POST)
        - headers: <headers_to_pass_to_graphql_session>
        - auth: <auth_to_pass_to_graphql_session>
        - query: <stringified_graphql_query_or_mutation_to_execute>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        '''

    async def create_session(self, actions=AsyncList()):
        pass

    async def yield_session(self):
        pass

    async def execute(self, action):
        return await action.execute(action)

    async def defer_all(self, actions):
        async for action in actions:
            yield wrap_awaitable_future(
                action,
                action
            )

    @async_execute_or_catch()
    async def close(self):
        for teardown_action in self._teardown_actions:
            await teardown_action.execute(self.session)
        