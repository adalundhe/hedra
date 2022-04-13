from .sessions import GraphQLSession
from .mercury_engine import MercuryHTTPEngine


class GraphQLEngine(MercuryHTTPEngine):

    def __init__(self, config, handler):
        super().__init__(config, handler)
        self.session = GraphQLSession(
            pool_size=self._pool_size,
            dns_cache_seconds=10**8,
            request_timeout=self.config.get('request_timeout'),
            session_url=self.config.get('session_url')
        )

    @classmethod
    def about(cls):
        return '''
        GraphQL Engine - (graphql)

        The GraphQL engine executes GraphQL queries against any valid GraphQL endpoint using
        the gql (v3.0) library.

        Actions are specified as:

        - endpoint: <graphql_endpoint>
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - method: <GET_or_POST>
        - headers: <headers_to_pass_to_graphql_session>
        - auth: <auth_to_pass_to_graphql_session>
        - query: <stringified_graphql_query_or_mutation_to_execute>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        '''

    async def yield_session(self):
        return await self.session.create()
