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
from typing import Union, Optional, Tuple
from .validator import InjectJunkDataValidator


Request = Union[
    GraphQLAction,
    GraphQLHTTP2Action,
    GRPCAction,
    HTTPAction,
    HTTP2Action,
    HTTP3Action
]


class InjectJunkData(Mutation):

    def __init__(
        self, 
        name: str, 
        chance: float,
        *targets: Tuple[str, ...],
        junk_size: Optional[int]=None

    ) -> None:
        super().__init__(
            name, 
            chance,
            MutationType.INJECT_JUNK_DATA,
            *targets
        )

        validated_mutation = InjectJunkDataValidator(
            junk_size=junk_size
        )

        self.junk_size = validated_mutation.junk_size

        self._character_pool = ''.join([
            string.ascii_letters,
            string.digits,
            string.hexdigits,
            string.octdigits,
            string.punctuation,
            string.whitespace
        ])

        self.original_data: bytes = None

        junk_data: str = ''.join(
            random.choices(
                self._character_pool, 
                k=self.junk_size
            )
        )

        self.junk_data = junk_data.encode()

    async def mutate(self, action: Request=None) -> Request:

        chance_roll = random.uniform(0, 1)
        if chance_roll <= self.chance:
            return action

        if action.method not in ['POST', 'PUT', 'PATCH']:
            return action

        if self.original_data is None:
            self.original_data = action.encoded_data

        action.encoded_data = self.original_data + self.junk_data
        action._setup_data()
        action._setup_headers()
        
        return action
    
    def copy(self):
        return InjectJunkData(
            self.name,
            self.chance,
            *list(self.targets),
            junk_size=self.junk_size
        )