from types import FunctionType
from typing import Any, Dict, List
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.common.types import RequestTypes
from .base import Action


class GRPCAction(Action):

    def __init__(
        self, 
        name: str, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        protobuf: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType]=[]
    ) -> None:
        self.data = Request(
            name,
            url,
            method=method,
            headers=headers,
            payload=protobuf,
            user=user,
            tags=tags,
            checks=checks,
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

    async def setup(self):
        self.data.setup_grpc_request()
        await self.data.url.lookup()
        self.is_setup = True