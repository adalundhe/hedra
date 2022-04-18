from .mercury_http_action import MercuryHTTPAction


class MercuryWebsocketAction(MercuryHTTPAction):

    def __init__(self, action, group=None, session=None):
        super(
            MercuryWebsocketAction,
            self
        ).__init__(action, group, session)
        self.action_type = 'websocket'


    @classmethod
    def about(cls):
        return '''
        Mercury Websocket Action

        Mercury Websocket Actions represent a single cycle of connecting to, receiving/sending, and
        disconnecting from the websocket at the specified uri.

        Actions are specified as:

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

    async def setup(self) -> None:
        await self.request.setup_websocket_request()
        await self.request.url.lookup()
        