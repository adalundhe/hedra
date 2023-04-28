import json
import string
import random
from hedra.core.engines.types.graphql.action import GraphQLAction
from hedra.core.engines.types.graphql_http2.action import GraphQLHTTP2Action
from hedra.core.engines.types.grpc.action import GRPCAction
from hedra.core.engines.types.http.action import HTTPAction
from hedra.core.engines.types.http2.action import HTTP2Action
from hedra.core.engines.types.http3.action import HTTP3Action
from hedra.core.experiments.mutations.types.base.mutation import Mutation
from hedra.core.experiments.mutations.types.base.mutation_type import MutationType
from typing import Union, Optional, Dict, Tuple
from .validator import SmuggleRequestValidator


Request = Union[
    GraphQLAction,
    GraphQLHTTP2Action,
    GRPCAction,
    HTTPAction,
    HTTP2Action,
    HTTP3Action
]


class SmuggleRequest(Mutation):

    def __init__(
        self, 
        name: str, 
        chance: float,
        *targets: Tuple[str, ...],
        request_size: Optional[int]=None,
        smuggled_request: Optional[Union[Dict[str, str], bytes, str]]=None

    ) -> None:
        super().__init__(
            name, 
            chance,
            MutationType.SMUGGLE_REQUEST,
            *targets
        )

        validated_mutation = SmuggleRequestValidator(
            request_size=request_size,
            smuggled_request=smuggled_request
        )

        self.request_size = validated_mutation.request_size
        self.smuggled_request = validated_mutation.smuggled_request

        encoded_smuggled_request = self.smuggled_request

        if isinstance(encoded_smuggled_request, dict):
            encoded_smuggled_request = json.dumps(encoded_smuggled_request)
        
        if isinstance(encoded_smuggled_request, str):
            encoded_smuggled_request = encoded_smuggled_request.encode()

        self._encoded_smuggled_request = encoded_smuggled_request
        self._character_pool = ''.join([
            string.ascii_letters,
            string.digits,
            string.hexdigits,
            string.octdigits,
            string.punctuation,
            string.whitespace
        ])

        self.original_data: bytes = None

    async def mutate(self, action: Request=None) -> Request:

        chance_roll = random.uniform(0, 1)
        if chance_roll <= self.chance:
            return action

        if action.method not in ['POST', 'PUT', 'PATCH']:
            return action
        
        lowered_headers = {
            header_name.lower(): header_value for header_name, header_value in action.headers.items()
        }

        update_headers = {}
        if lowered_headers.get('content-type') is None:
            update_headers['Content-Type'] = len(action.encoded_data)

        if lowered_headers.get('transfer-encoding') is None:
            update_headers['Transfer-Encoding'] = 'chunked'
            
        action._headers.update(update_headers)
        action._header_items = list(action._headers.items())

        action._setup_headers()

        if self.smuggled_request is None:
            encoded_smuggled_request: str = ''.join(
                random.choices(
                    self._character_pool, 
                    k=self.request_size
                )
            )

            self._encoded_smuggled_request = encoded_smuggled_request.encode()

        if self.original_data is None:
            self.original_data = action.encoded_data
        
        action.encoded_data = self.original_data + self._encoded_smuggled_request  

        return action
    
    def copy(self):
        return SmuggleRequest(
            self.name,
            self.chance,
            *list(self.targets),
            request_size=self.request_size,
            smuggled_request=self.smuggled_request
        )