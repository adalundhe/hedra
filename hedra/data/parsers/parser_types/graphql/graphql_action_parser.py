import uuid
import json
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.graphql import (
    GraphQLAction,
    MercuryGraphQLClient
)
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.data.parsers.parser_types.common.base_parser import BaseParser
from hedra.data.parsers.parser_types.common.parsing import (
    normalize_headers,
    parse_tags
)
from typing import Any, Coroutine, Dict
from .graphql_action_validator import GraphQLActionValidator


class GraphQLActionParser(BaseParser):

    def __init__(
        self,
        config: Config,
        options: Dict[str, Any]={}
    ) -> None:
        super().__init__(
            GraphQLActionParser.__name__,
            config,
            RequestTypes.GRAPHQL,
            options
        )

    async def parse(
        self, 
        action_data: Dict[str, Any],
        stage: str
    ) -> Coroutine[Any, Any, Coroutine[Any, Any, ActionHook]]:
        
        graphql_variables_data = action_data.get('variables')
        if isinstance(graphql_variables_data, (str, bytes, bytearray,)):
            graphql_variables_data = json.loads(graphql_variables_data)

        normalized_headers = normalize_headers(action_data)
        tags_data = parse_tags(action_data)

        generator_action = GraphQLActionValidator(**{
            **action_data,
            'headers': normalized_headers,
            'variables': graphql_variables_data,
            'tags': tags_data
        })

        action = GraphQLAction(
            generator_action.name,
            generator_action.url,
            method=generator_action.method,
            headers=generator_action.headers,
            data={
                'query': generator_action.query,
                'operation_name': generator_action.operation_name,
                'variables': generator_action.variables
            },
            user=generator_action.user,
            tags=[
                tag.dict() for tag in generator_action.tags
            ]
        )

        session = MercuryGraphQLClient(
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
            sourcefile=generator_action.sourcefile
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



