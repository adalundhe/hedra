from __future__ import annotations
from types import FunctionType
from typing import Any, Dict, List
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.common.types import RequestTypes
from .base import Action


class HTTP2Action(Action):

    def __init__(
        self, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        data: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType]=[]
    ) -> None:
        super().__init__()
        self.url = url
        self.method = method
        self.headers = headers
        self.data = data
        self.user = user
        self.tags = tags
        self.checks = checks
        self.parsed: Request = None

    def to_type(self, name: str):
        self.parsed = Request(
            name,
            self.url,
            method=self.method,
            headers=self.headers,
            payload=self.data,
            user=self.user,
            tags=self.tags,
            checks=self.checks,
            before=self.before,
            after=self.after,
            request_type=RequestTypes.HTTP2
        )

    @classmethod
    def about(cls):
        return '''
        Mercury-HTTP2 Action

        Mercury-HTTP2 Actions represent a single HTTP/2 REST call using Hedra's Mercury-HTTP2
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

        '''