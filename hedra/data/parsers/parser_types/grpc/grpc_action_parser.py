import uuid
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.grpc import (
    GRPCAction,
    MercuryGRPCClient
)
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.grpc.protobuf_registry import protobuf_registry
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.data.parsers.parser_types.common.base_parser import BaseParser
from hedra.data.parsers.parser_types.common.parsing import (
    normalize_headers,
    parse_data,
    parse_tags
)
from typing import Any, Coroutine, Dict
from .grpc_action_validator import GRPCActionValidator
from .grpc_options_validator import GRPCOptionsValidator


class GRPCActionParser(BaseParser):

    def __init__(
        self,
        config: Config,
        options: Dict[str, Any]={}
    ) -> None:
        super().__init__(
            GRPCActionParser.__name__,
            config,
            RequestTypes.GRPC,
            options
        )

        self.grpc_options = GRPCOptionsValidator(
            protobuf_map=protobuf_registry
        )

        self.protobuf_map = self.grpc_options.protobuf_map

    async def parse(
        self, 
        action_data: Dict[str, Any],
        stage: str
    ) -> Coroutine[Any, Any, Coroutine[Any, Any, ActionHook]]:
        
        action_name = action_data.get('name')
        normalized_headers = normalize_headers(action_data)
        tags_data = parse_tags(action_data)

        protobuf = self.grpc_options.protobuf_map[action_name].ParseFromString(
            action_data.get('data')
        )

        generator_action = GRPCActionValidator(**{
            **action_data,
            'headers': normalized_headers,
            'data': protobuf,
            'tags': tags_data
        })

        action = GRPCAction(
            generator_action.name,
            generator_action.url,
            method=generator_action.method,
            headers=generator_action.headers,
            data=generator_action.data,
            user=generator_action.user,
            tags=[
                tag.dict() for tag in generator_action.tags
            ]
        )

        session = MercuryGRPCClient(
            concurrency=self.config.batch_size,
            timeouts=self.timeouts,
            reset_connections=self.config.reset_connections,
            tracing_session=self.config.tracing
        )

        await session.prepare(action)
        
        hook = ActionHook(
            f'{stage}.{generator_action.name}',
            generator_action.name,
            None,
            order=generator_action.order,
            weight=generator_action.weight,
            metadata={
                'user': generator_action.user,
                'tags': generator_action.tags   
            }
        )

        hook.session = session
        hook.action = action
        hook.stage = stage
        hook.context = SimpleContext()
        hook.hook_id = uuid.uuid4()

        hook.metadata.order = generator_action.order
        hook.metadata.weight = generator_action.weight
        hook.metadata.tags = generator_action.tags
        hook.metadata.user = generator_action.user


        return hook



