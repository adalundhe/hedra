from typing import Any, Dict, List, Union
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.grpc import (
    MercuryGRPCClient,
    GRPCAction,
    GRPCResult
)
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.client.store import ActionsStore
from hedra.core.engines.types.tracing.trace_session import (
    TraceSession, 
    Trace
)
from hedra.logging import HedraLogger
from .base_client import BaseClient


class GRPCClient(BaseClient[MercuryGRPCClient, GRPCAction, GRPCResult]):
    
    def __init__(self, config: Config) -> None:
        super().__init__()

        if config is None:
            config = Config()

        tracing_session: Union[TraceSession, None] = None
        if config.tracing:
            trace_config_dict = config.tracing.to_dict()
            tracing_session = TraceSession(**trace_config_dict)

        self.session = MercuryGRPCClient(
            concurrency=config.batch_size,
            timeouts=Timeouts(
                total_timeout=config.request_timeout
            ),
            reset_connections=config.reset_connections,
            tracing_session=tracing_session
        )
        self.request_type = RequestTypes.GRPC
        self.client_type = self.request_type.capitalize()

        self.actions: ActionsStore = None
        self.next_name = None
        self.intercept = False

        self.logger = HedraLogger()
        self.logger.initialize()

    def __getitem__(self, key: str):
        return self.session.registered.get(key)

    async def request(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        protobuf: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [],
        trace: Trace=None
    ):
        if trace and self.session.tracing_session is None:
            self.session.tracing_session = TraceSession(
                **trace.to_dict()
            )

        request = GRPCAction(
            self.next_name,
            url,
            method='POST',
            headers=headers,
            data=protobuf,
            user=user,
            tags=tags
        )

        return await self._execute_action(request)