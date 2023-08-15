from hedra.core.engines.client.config import Config
from hedra.core.engines.types.http3 import (
    HTTP3Action,
    HTTP3Result
)
from hedra.core.engines.types.common.types import RequestTypes
from hedra.data.parsers.parser_types.common.base_parser import BaseParser
from hedra.data.parsers.parser_types.common.result_validator import ResultValidator
from hedra.data.parsers.parser_types.common.parsing import (
    normalize_headers,
    parse_data,
    parse_tags
)
from typing import Any, Coroutine, Dict
from .http3_action_validator import HTTP3ActionValidator


class HTTP3ResultParser(BaseParser):

    def __init__(
        self,
        config: Config,
        options: Dict[str, Any]={}
    ) -> None:
        super().__init__(
            HTTP3ResultParser.__name__,
            config,
            RequestTypes.HTTP3,
            options
        )

    async def parse(
        self, 
        result_data: Dict[str, Any]
    ) -> Coroutine[Any, Any, Coroutine[Any, Any, HTTP3Action]]:
        
        normalized_headers = normalize_headers(result_data)
        content_type = normalized_headers.get('content-type')

        parsed_data = parse_data(
            result_data,
            content_type
        )

        tags_data = parse_tags(result_data)

        generator_action = HTTP3ActionValidator(
            engine=result_data.get('engine'),
            name=result_data.get('name'),
            url=result_data.get('url'),
            method=result_data.get('method'),
            headers=normalized_headers,
            params=result_data.get('params'),
            data=parsed_data,
            weight=result_data.get('weight'),
            order=result_data.get('order'),
            user=result_data.get('user'),
            tag=tags_data
        )


        action = HTTP3Action(
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


        result_validator = ResultValidator(
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

        result = HTTP3Result(
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



