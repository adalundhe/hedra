
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
from typing import Union, Optional, List, Tuple
from .validator import DeformHeaderValidator


Request = Union[
    GraphQLAction,
    GraphQLHTTP2Action,
    GRPCAction,
    HTTPAction,
    HTTP2Action,
    HTTP3Action
]


class DeformHeader(Mutation):

    def __init__(
        self, 
        name: str, 
        chance: float,
        *targets: Tuple[str, ...],
        header_name: str=None,
        header_value: Optional[str]=None,
        deformation_length: int=5,
        character_pool: Optional[List[str]]=[]
    ) -> None:
        super().__init__(
            name, 
            chance,
            MutationType.DEFORM_HEADER,
            *targets
        )

        validated_mutation = DeformHeaderValidator(
            header_name=header_name,
            deformation_length=deformation_length,
            character_pool=character_pool,
            header_value=header_value
        )

        self.header_name = validated_mutation.header_name
        self.header_value = validated_mutation.header_value
        self.deformation_length = validated_mutation.deformation_length

        if validated_mutation.character_pool:
            self.character_pool = ''.join(validated_mutation.character_pool)
            
        else:
            self.character_pool = ''.join([
                string.ascii_letters,
                string.digits,
                string.hexdigits,
                string.octdigits,
                string.punctuation,
                string.whitespace
            ])

        self.header_mutation = ''.join(
            random.choices(
                self.character_pool, 
                k=self.deformation_length
            )
        )

    async def mutate(self, action: Request=None) -> Request:

        chance_roll = random.uniform(0, 1)
        if chance_roll <= self.chance:
            return action

        
        mutated_header_name = f'{self.header_name} {self.header_mutation}'

        header_value = action.headers.get(self.header_name)

        if self.header_value:
            header_value = self.header_value

        del action._headers[self.header_name]

        action._headers[mutated_header_name] = header_value
        action._header_items = list(action._headers.items())

        action._setup_headers()
        
        return action
    
    def copy(self):
        return DeformHeader(
            self.name,
            self.chance,
            *list(self.targets),
            header_name=self.header_name,
            header_value=self.header_name,
            deformation_length=self.deformation_length,
            character_pool=self.character_pool
        )