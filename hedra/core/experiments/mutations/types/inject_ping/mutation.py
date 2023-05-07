import asyncio
import random
from hedra.core.engines.types.common.protocols import PingConnection
from hedra.core.engines.types.common.protocols.ping.ping_type import (
    PingTypesMap
)
from hedra.core.engines.types.graphql.action import GraphQLAction
from hedra.core.engines.types.graphql_http2.action import GraphQLHTTP2Action
from hedra.core.engines.types.grpc.action import GRPCAction
from hedra.core.engines.types.http.action import HTTPAction
from hedra.core.engines.types.http2.action import HTTP2Action
from hedra.core.engines.types.http3.action import HTTP3Action
from hedra.core.experiments.mutations.types.base.mutation import Mutation
from hedra.core.experiments.mutations.types.base.mutation_type import MutationType
from typing import Union, Tuple
from .validator import InjectPingValidator


Request = Union[
    GraphQLAction,
    GraphQLHTTP2Action,
    GRPCAction,
    HTTPAction,
    HTTP2Action,
    HTTP3Action
]


class InjectPing(Mutation):

    def __init__(
        self, 
        name: str, 
        chance: float,
        *targets: Tuple[str, ...],
        ping_type: str='icmp',
        timeout: int=2
    ) -> None:
        super().__init__(
            name, 
            chance,
            MutationType.INJECT_PING,
            *targets
        )

        validated_mutation = InjectPingValidator(
            ping_type=ping_type,
            timeout=timeout
        )

        self.ping_connection = PingConnection()
        self.types_map = PingTypesMap()
        self.ping_type = self.types_map.get(validated_mutation.ping_type)
        self.timeout = validated_mutation.timeout

    async def mutate(self, action: Request=None) -> Request:
        chance_roll = random.uniform(0, 1)
        if chance_roll <= self.chance:
            return action
        
        try:
            await asyncio.wait_for(
                self.ping_connection.ping(
                    action.url.socket_config,
                    ping_type=self.ping_type,
                    timeout=self.timeout
                ),
                timeout=self.timeout
            )
        except Exception:
            pass

        return action
    
    def copy(self):
        return InjectPing(
            self.name,
            self.chance,
            *list(self.targets),
            ping_type=self.types_map.get_name(
                self.ping_type
            ),
            timeout=self.timeout
        )