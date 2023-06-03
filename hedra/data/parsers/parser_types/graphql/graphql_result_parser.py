import uuid
import json
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.graphql import (
    GraphQLAction,
    GraphQLResult,
    MercuryGraphQLClient
)
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.data.parsers.parser_types.common.base_parser import BaseParser
from hedra.data.parsers.parser_types.common.parsing import (
    normalize_headers,
    parse_tags
)
from typing import Any, Coroutine, Dict
from .graphql_action_validator import GraphQLActionValidator
from .graphql_result_validator import GraphQLResultValidator


class GraphQLResultParser(BaseParser):

    def __init__(
        self,
        config: Config,
        options: Dict[str, Any]={}
    ) -> None:
        super().__init__(
            GraphQLResultParser.__name__,
            config,
            RequestTypes.GRAPHQL,
            options
        )

    async def parse(
        self, 
        result_data: Dict[str, Any]
    ) -> Coroutine[Any, Any, Coroutine[Any, Any, GraphQLResult]]:
        
        graphql_variables_data = result_data.get('variables')
        if isinstance(graphql_variables_data, (str, bytes, bytearray,)):
            graphql_variables_data = json.loads(graphql_variables_data)

        normalized_headers = normalize_headers(result_data)
        tags_data = parse_tags(result_data)

        generator_action = GraphQLActionValidator(
            engine=result_data.get('engine'),
            name=result_data.get('name'),
            url=result_data.get('url'),
            method=result_data.get('method'),
            headers=normalized_headers,
            query=result_data.get('query'),
            operation_name=result_data.get('operation_name'),
            variables=graphql_variables_data,
            weight=result_data.get('weight'),
            order=result_data.get('order'),
            user=result_data.get('user'),
            tag=tags_data
        )

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


        result_validator = GraphQLResultValidator(
            error=result_data.get('error'),       
            status=result_data.get('status'),
            reason=result_data.get('reason'),
            params=result_data.get('params'),
            wait_start=result_data.get('wait_start'),
            start=result_data.get('start'),
            connect_end=result_data.get('connect_end'),
            write_end=result_data.get('write_end'),
            complete=result_data.get('complete'),
            checks=result_data.get('checks')
        )

        result = GraphQLResult(
            action,
            error=Exception(result_validator.error) if result_validator.error else None
        )

        result.query = result_validator.query
        result.status = result_validator.status
        result.reason = result_validator.reason
        result.params = result_validator.params
        result.wait_start = result_validator.wait_start
        result.start = result_validator.start
        result.connect_end = result_validator.connect_end
        result.write_end = result_validator.write_end
        result.complete = result_validator.complete
        result.checks = result_validator.checks

        return result



