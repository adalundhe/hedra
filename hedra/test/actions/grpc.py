from types import FunctionType
from typing import Any, Dict, List
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.common.types import RequestTypes
from .base import Action


class GRPCAction(Action):

    def __init__(
        self, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        protobuf: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType]=[]
    ) -> None:
        super().__init__()
        self.url = url
        self.method = method
        self.headers = headers
        self.protobuf = protobuf
        self.user = user
        self.tags = tags
        self.checks = checks

    def to_type(self, name: str):
        self.parsed = Request(
            name,
            self.url,
            method=self.method,
            headers=self.headers,
            payload=self.protobuf,
            user=self.user,
            tags=self.tags,
            checks=self.checks,
            before=self.before,
            after=self.after,
            request_type=RequestTypes.GRPC
        )

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