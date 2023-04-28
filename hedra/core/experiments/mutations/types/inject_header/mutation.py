import random
from hedra.core.engines.types.graphql.action import GraphQLAction
from hedra.core.engines.types.graphql_http2.action import GraphQLHTTP2Action
from hedra.core.engines.types.grpc.action import GRPCAction
from hedra.core.engines.types.http.action import HTTPAction
from hedra.core.engines.types.http2.action import HTTP2Action
from hedra.core.engines.types.http3.action import HTTP3Action
from hedra.core.experiments.mutations.types.base.mutation import Mutation
from hedra.core.experiments.mutations.types.base.mutation_type import MutationType
from typing import Union, Tuple, List
from .validator import InjectHeaderValidator


Request = Union[
    GraphQLAction,
    GraphQLHTTP2Action,
    GRPCAction,
    HTTPAction,
    HTTP2Action,
    HTTP3Action
]


class InjectHeader(Mutation):

    def __init__(
        self, 
        name: str, 
        chance: float,
        *targets: Tuple[str, ...],
        header_name: str=None,
        header_value: Union[str, bytes, bytearray]=None
    ) -> None:
        super().__init__(
            name, 
            chance,
            MutationType.INJECT_HEADER,
            *targets
        )

        validated_mutation = InjectHeaderValidator(
            header_name=header_name,
            header_value=header_value
        )

        self.header_name = validated_mutation.header_name
        self.header_value = validated_mutation.header_value
        self.original_headers: Union[str, List[Tuple[str, str]]]

    async def mutate(self, action: Request=None) -> Request:
        chance_roll = random.uniform(0, 1)
        if chance_roll <= self.chance:
            return action
        
        original_headers = list(action._header_items)

        action._header_items: List[Tuple[str, str]] = original_headers
        mutation_header = (
            self.header_name,
            self.header_value
        )

        if mutation_header not in action._header_items:
            action._header_items.append(mutation_header)
            action._setup_headers()

        action._headers[self.header_name] = self.header_value

        return action
    
    def copy(self):
        return InjectHeader(
            self.name,
            self.chance,
            *list(self.targets),
            header_name=self.header_name,
            header_value=self.header_value
        )