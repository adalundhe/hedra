from .utils import (
    convert_data,
    stringify_params
)

from mercury_http.http import MercuryHTTPClient

class MercuryHTTPAction:

    def __init__(self, action, group=None):
        self.name = action.get('name')
        self.host = action.get('host', group)
        self.endpoint = action.get('endpoint')
        self.user = action.get('user')
        self.tags = action.get('tags', [])
        self.url = action.get(
            'url',
            f'{self.host}{self.endpoint}'
        )
        self.method = action.get('method')
        self.headers = action.get('headers', {})
        self.params = action.get('params')
        self.auth = action.get('auth')
        self.data = action.get('data')
        self.weight = action.get('weight', 1)
        self.order = action.get('order', 1)
        self.action_type = 'http'
        self.is_setup = action.get('is_setup', False)
        self.is_teardown = action.get('is_teardown', False)
        self.ssl = action.get('ssl', False)
        self.action_type = 'mercury-http'

    @classmethod
    def about(cls):
        return '''
        Mercury-HTTP Action

        Mercury-HTTP Actions represent a single HTTP 1.X REST call using Hedra's Mercury-HTTP
        engine. For example - a GET request to https://www.google.com/.

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
        - ssl: <boolean_to_use_ssl> (defaults to False)
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        '''

    async def update_headers(self, headers) -> dict:
        return self.headers.update(headers)
        
    async def parse_data(self) -> None:
        self.params = await stringify_params(
            params=self.params
        )
        self.data = await convert_data(
            self.data,
            method=self.method,
            mime_type=self.headers.get('Content-Type')
        )

    def execute(self, session: MercuryHTTPClient):
        return session.request(
            self.name,
            self.url,
            method=self.method,
            headers=self.headers,
            params=self.params,
            data=self.data,
            ssl=self.ssl
        )

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'host': self.host,
            'endpoint': self.endpoint,
            'user': self.user,
            'tags': self.tags,
            'url': self.url,
            'method': self.method,
            'headers': self.headers,
            'params': self.params,
            'auth': self.auth,
            'data': self.data,
            'order': self.order,
            'weight': self.weight,
            'action_type': self.action_type
        }