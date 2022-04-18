from .sessions import MercuryWebsocketSession
from .mercury_http_engine import MercuryHTTPEngine


class MercuryWebsocketEngine(MercuryHTTPEngine):

    def __init__(self, config, handler):
        super(
            MercuryHTTPEngine,
            self
        ).__init__(config, handler)
        self.session = MercuryWebsocketSession(
            pool_size=self.config.get('batch_size', 10**3),
            request_timeout=self.config.get('request_timeout'),
            hard_cache=self.config.get('hard_cache'),
            reset_connections=self.config.get('reset_connections')
        )


    @classmethod
    def about(cls):
        return '''
        Mercury Websocket Engine - (websocket)

        The Mercury Websocket Engine makes requests to websockets - each request connecting, receiving/sending
        then disconnecting. This is intentional - designed to test how well websockets handle large amounts 
        of varied traffic.

        Actions are specified as:

        - endpoint: <host_endpoint>
        - url: <full_url_to_target>
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
    