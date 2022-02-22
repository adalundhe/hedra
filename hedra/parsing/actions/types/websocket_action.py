class WebsocketAction:

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
        self.auth = action.get('auth')
        self.data = action.get('data')
        self.weight = action.get('weight', 1)
        self.order = action.get('order', 1)
        self.action_type = 'websocket'
        self.is_setup = action.get('is_setup', False)
        self.is_teardown = action.get('is_teardown', False)
        self._websocket_method = None
        self.args = []

        self.websocket_map = {
            'POST': {
                'application/json': 'send_json',
                'text/html': 'send_str',
                'default': 'send_bytes'
            },
            'GET': {
                'application/json': 'receive_json',
                'application/octet-stream': 'receive_bytes',
                'text/html': 'receive_str',
                'default': 'receive'
            }
        }

    @classmethod
    def about(cls):
        return '''
        Websocket Action

        Websocket Actions represent a single cycle of connecting to, receiving/sending, and
        disconnecting from the websocket at the specified uri.

        Actions are specified as:

        - endpoint: <host_endpoint>
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - url: (optional) <full_target_url> (overrides host and endpoint if provided)
        - method: <webocket_request_method> (must be GET or POST)
        - headers: <websocket_request_headers>
        - auth: <websocket_request_auth>
        - params: <websocket_request_params>
        - data: <websocket_request_data>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        '''

    async def parse_data(self):
        websocket_method_map = self.websocket_map.get(
            self.method,
            self.websocket_map.get('GET') 
        )

        self._websocket_method = websocket_method_map.get(
            self.headers.get('Content-Type'),
            websocket_method_map.get('default')
        )

        self.websocket = None
        if self.method == 'POST':
            self.args = [self.data]

    async def execute(self, session):
        async with session.ws_connect(
            self.url, 
            method=self.method, 
            headers=self.headers,
            auth=self.auth
        ) as websocket:
            return await websocket.__getattribute__(self._websocket_method)(*self.args)

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
            'auth': self.auth,
            'data': self.data,
            'order': self.order,
            'weight': self.weight,
            'action_type': self.action_type
        }

        