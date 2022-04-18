from mercury_http.grpc import MercuryGRPCClient
from mercury_http.common import Request


class MercuryGRPCAction:

    def __init__(self, action, group=None, session=None):

        self.request = Request(
            action.get('name'),
            action.get('url'),
            method=action.get('method'),
            headers=action.get('headers', {}),
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
        self.action_type = 'grpc'

    @classmethod
    def about(cls):
        return '''
        Mercury-GRPC Action

        Mercury-GRPC Actions represent a single GRPC call using Hedra's Mercury-GRPC engine.

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
        
    async def setup(self) -> None:
        await self.request.setup_http_request()
        await self.request.url.lookup()
    
    def execute(self, session: MercuryGRPCClient):
        return session.request(self.request)

    def to_dict(self) -> dict:
        return {
            'request': self.request,
            'order': self.order,
            'weight': self.weight,
            'action_type': self.action_type
        }