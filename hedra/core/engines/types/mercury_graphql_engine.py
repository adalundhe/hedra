from typing import AsyncIterator
from hedra.parsing.actions.types.mercury_graphql_action import MercuryGraphQLAction
from .mercury_http_engine import MercuryHTTPEngine
from .sessions import MercuryGraphQLSession


class MercuryGraphQLEngine(MercuryHTTPEngine):
    
    def __init__(self, config, handler):
        super(
            MercuryGraphQLEngine,
            self
        ).__init__(config, handler)
        
        self.session = MercuryGraphQLSession(
            pool_size=self.config.get('batch_size', 10**3),
            request_timeout=self.config.get('request_timeout'),
            hard_cache=self.config.get('hard_cache'),
            use_http2=self.config.get('use_http2', False),
            reset_connections=self.config.get('reset_connections')
        )

    @classmethod
    def about(cls):
        return '''
        Mercury GraphQL Engine - (graphql)

        The Mercury GraphQL engine executes GraphQL queries against any valid GraphQL endpoint.

        Actions are specified as:

        - endpoint: <graphql_endpoint>
        - url: <full_url_to_target>
        - method: <GET_or_POST>
        - headers: <headers_to_pass_to_graphql_session>
        - auth: <auth_to_pass_to_graphql_session>
        - data: <dict_containing_query_and_variable_key_value_pairs>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        '''

    async def prepare(self, actions: AsyncIterator[MercuryGraphQLAction]):
        for action in actions:
            await self.session.prepare_request(action.request)
