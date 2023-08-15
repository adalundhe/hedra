import uuid
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.http2 import (
    HTTP2Action,
    MercuryHTTP2Client
)

from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.data.parsers.parser_types.common.base_parser import BaseParser
from hedra.data.parsers.parser_types.common.parsing import (
    normalize_headers,
    parse_data
)
from typing import Any, Coroutine, Dict
from .http2_action_validator import HTTP2ActionValidator


class HTTP2ActionParser(BaseParser):

    def __init__(
        self,
        config: Config,
        options: Dict[str, Any]={}
    ) -> None:
        super().__init__(
            HTTP2ActionParser.__name__,
            config,
            RequestTypes.HTTP2,
            options
        )

    async def parse(
        self, 
        action_data: Dict[str, Any],
        stage: str
    ) -> Coroutine[Any, Any, Coroutine[Any, Any, ActionHook]]:
        
        normalized_headers = normalize_headers(action_data)
        parsed_data = parse_data(action_data)
        tags_data = parse_data(action_data)

        generator_action = HTTP2ActionValidator(**{
            **action_data,
            'headers': normalized_headers,
            'data': parsed_data,
            'tags': tags_data
        })

        action = HTTP2Action(
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

        session = MercuryHTTP2Client(
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
            sourcefile=generator_action.sourcefile,
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



