from types import FunctionType
from typing import Any, Dict, List
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.common.types import RequestTypes
from .base import Action


class WebsocketAction(Action):
    
    def __init__(
        self, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {}, 
        data: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType]=[]
    ) -> None:
        super().__init__()
        self.url = url
        self.method = method
        self.headers = headers
        self.params = params
        self.data = data
        self.user = user
        self.tags = tags
        self.checks = checks

    def to_type(self, name: str):
        self.data = Request(
            name,
            self.url,
            method=self.method,
            headers=self.headers,
            params=self.params,
            payload=self.data,
            user=self.user,
            tags=self.tags,
            checks=self.checks,
            before=self.before,
            after=self.after,
            request_type=RequestTypes.WEBSOCKET
        )

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
        - params: <websocket_request_params>
        - data: <websocket_request_data>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        '''