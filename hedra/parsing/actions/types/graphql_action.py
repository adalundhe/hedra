from gql import gql


class GraphQLAction:

    def __init__(self, action, group=None) -> None:
        self.name = action.get('name')
        self.endpoint = action.get('endpoint')
        self.host = action.get('host', group)
        self.query_string = action.get('query')
        self.type = action.get('type')
        self.method = action.get('method')
        self.url = action.get(
            'url',
            f'{self.host}{self.endpoint}'
        )
        self.order = action.get('order')
        self.weight = action.get('weight')
        self.user = action.get('user')
        self.tags = action.get('tags', [])
        self.headers = action.get('headers', {})
        self.auth = action.get('auth')
        self.is_setup = action.get('is_setup', False)
        self.is_teardown = action.get('is_teardown', False)

        self.action_type = 'graphql'
        self.query = None

    @classmethod
    def about(cls):
        return '''
        Graphql Action

        GraphQL Actions represent a single GraphQL query or mutation.

        Actions are specified as:

        - endpoint: <graphql_endpoint>
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - url: (optional) <full_target_url> (overrides host and endpoint if provided)
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

    async def execute(self, session):
        session.auth = self.auth
        session.headers = self.headers
        return await session.execute(self.query)

    async def parse_data(self):
        self.query = gql(self.query_string)
            
    def to_dict(self):
        return {
            'name': self.name,
            'host': self.host,
            'query': self.query_string,
            'endpoint': self.endpoint,
            'user': self.user,
            'tags': self.tags,
            'url': self.url,
            'method': self.method,
            'headers': self.headers,
            'auth': self.auth,
            'order': self.order,
            'weight': self.weight,
            'action_type': self.action_type
        }