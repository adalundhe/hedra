from hedra.core.engines.client.config import Config
from hedra.core.engines.types.udp import (
    UDPAction,
    UDPResult
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
from .udp_action_validator import UDPActionValidator


class UDPResultParser(BaseParser):

    def __init__(
        self,
        config: Config,
        options: Dict[str, Any]={}
    ) -> None:
        super().__init__(
            UDPResultParser.__name__,
            config,
            RequestTypes.UDP,
            options
        )

    async def parse(
        self, 
        result_data: Dict[str, Any]
    ) -> Coroutine[Any, Any, Coroutine[Any, Any, UDPResult]]:
        
        parsed_data = parse_data(result_data)
        tags_data = parse_tags(result_data)

        generator_action = UDPActionValidator(
            engine=result_data.get('engine'),
            name=result_data.get('name'),
            url=result_data.get('url'),
            wait_for_response=result_data.get('wait_for_response'),
            data=parsed_data,
            weight=result_data.get('weight'),
            order=result_data.get('order'),
            user=result_data.get('user'),
            tags=tags_data
        )

        action = UDPAction(
            generator_action.name,
            generator_action.url,
            wait_for_response=generator_action.wait_for_response,
            data=generator_action.data,
            user=generator_action.user,
            tags=[
                tag.dict() for tag in generator_action.tags
            ]
        )

        body = result_data.get('body')
        if isinstance(body, str):
            body = body.encode()

        result_validator = ResultValidator(
            error=result_data.get('error'), 
            body=body,      
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

        result = UDPResult(
            action,
            error=Exception(result_validator.error) if result_validator.error else None
        )

        result.body = result_validator.body
        result.status = result_validator.status
        result.params = result_validator.params
        result.wait_start = result_validator.wait_start
        result.start = result_validator.start
        result.connect_end = result_validator.connect_end
        result.write_end = result_validator.write_end
        result.complete = result_validator.complete
        result.checks = result_validator.checks

        return result



