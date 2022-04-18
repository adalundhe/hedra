from mercury_http.graphql import MercuryGraphQLClient
from mercury_http.common import Request


class MercuryGraphQLAction:

    def __init__(self, action, group=None, session=None):

        self.request = Request(
            action.get('name'),
            action.get('url'),
            method=action.get('method'),
            headers=action.get('headers', {}),
            params=action.get('params', {}),
            payload=action.get('data'),
            checks=action.get('checks')
        )

        self.request.metadata.user = action.get('user')
        self.request.metadata.tags = action.get('tags', [])
        
        self.weight = action.get('weight', 1)
        self.order = action.get('order', 1)
        self.is_setup = action.get('is_setup', False)
        self.is_teardown = action.get('is_teardown', False)
        self.group = group
        self.action_type = 'graphql'
        self.use_http2 = action.get('http2', False)

    @classmethod
    def about(cls):
        return '''
        Mercury-GraphQL Action

        Mercury-GraphQL Actions represent a single HTTP/1 or HTTP/2 REST call using Hedra's 
        Mercury-GraphQL engine. For example - a GET request to https://www.google.com/.

        Actions are specified as:

        - endpoint: <host_endpoint>
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - url: (optional) <full_target_url> (overrides host and endpoint if provided)
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
        
    async def setup(self) -> None:
        await self.request.setup_graphql_request(
            use_http2=self.use_http2
        )
        await self.request.url.lookup()
    
    def execute(self, session: MercuryGraphQLClient):
        return session.request(self.request)

    def to_dict(self) -> dict:
        return {
            'request': self.request,
            'order': self.order,
            'weight': self.weight,
            'action_type': self.action_type
        }