from .mercury_engine import MercuryHTTPEngine


class WebsocketEngine(MercuryHTTPEngine):

    def __init__(self, config, handler):
        super().__init__(config, handler)

    @classmethod
    def about(cls):
        return '''
        Websocket Engine - (websocket)

        The Websocket Engine makes requests to websockets - each request connecting, receiving/sending
        then disconnecting. This is intentional - designed to test how well websockets handle large amounts 
        of varied traffic.

        Actions are specified as:

        - endpoint: <host_endpoint>
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
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
    