import uuid
import json
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.playwright import (
    PlaywrightCommand,
    Page,
    Input,
    URL,
    Options,
    PlaywrightResult
)
from hedra.core.engines.types.common.types import RequestTypes
from hedra.data.parsers.parser_types.common.base_parser import BaseParser
from hedra.data.parsers.parser_types.common.result_validator import ResultValidator
from hedra.data.parsers.parser_types.common.parsing import (
    normalize_headers,
    parse_tags
)
from typing import Any, Coroutine, Dict
from .playwright_action_validator import (
    PlaywrightActionValidator,
    PlaywrightInputValidator,
    PlaywrightOptionsValidator,
    PlaywrightPageValidator,
    PlaywrightURLValidator
)



class PlaywrightResultParser(BaseParser):

    def __init__(
        self,
        config: Config,
        options: Dict[str, Any]={}
    ) -> None:
        super().__init__(
            PlaywrightResultParser.__name__,
            config,
            RequestTypes.UDP,
            options
        )

    async def parse(
        self, 
        result_data: Dict[str, Any]
    ) -> Coroutine[Any, Any, Coroutine[Any, Any, PlaywrightResult]]:
        
        normalized_headers = normalize_headers(result_data)
        tags_data = parse_tags(result_data)

        playwright_input_args = result_data.get('args')
        if isinstance(playwright_input_args, (str, bytes, bytearray,)):
            playwright_input_args = playwright_input_args.split(',')

        playwright_options_extra = result_data.get('extra')
        if isinstance(playwright_options_extra, (str, bytes, bytearray,)):
            playwright_options_extra = json.loads(playwright_options_extra)

        generator_action = PlaywrightActionValidator(
            name=result_data.get('name'),
            command=result_data.get('command'),
            page=PlaywrightPageValidator(
                selector=result_data.get('selector'),
                attribute=result_data.get('attribute'),
                x_coordinate=result_data.get('x_coordinate'),
                y_coordinate=result_data.get('y_coordinate'),
                frame=result_data.get('frame')
            ),
            url=PlaywrightURLValidator(
                location=result_data.get('location'),
                headers=normalized_headers
            ),
            input=PlaywrightInputValidator(
                key=result_data.get('key'),
                text=result_data.get('text'),
                expression=result_data.get('expression'),
                args=playwright_input_args,
                filepath=result_data.get('filepath'),
                file=result_data.get('file'),
                path=result_data.get('path'),
                option=result_data.get('option'),
                by_label=result_data.get('by_label', False),
                by_value=result_data.get('by_value', False)
            ),
            options=PlaywrightOptionsValidator(
                event=result_data.get('event'),
                option=result_data.get('option'),
                is_checked=result_data.get('is_checked', False),
                timeout=result_data.get('timeout'),
                extra=playwright_options_extra,
                switch_by=result_data.get('switch_by')
            ),
            weight=result_data.get('weight'),
            order=result_data.get('order'),
            user=result_data.get('user'),
            tags=tags_data
        )

        action = PlaywrightCommand(
            generator_action.name,
            generator_action.command,
            url=URL(**generator_action.url.dict(
                exclude_none=True
            )),
            page=Page(**generator_action.page.dict(
                exclude_none=True
            )),
            input=Input(**generator_action.input.dict(
                exclude_none=True
            )),
            options=Options(**generator_action.options.dict(
                exclude_none=True
            )),
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

        result = PlaywrightResult(
            action,
            error=Exception(result_validator.error) if result_validator.error else None
        )

        result.checks = result_validator.checks
        result.wait_start = result_validator.wait_start
        result.start = result_validator.start
        result.connect_end = result_validator.connect_end
        result.write_end = result_validator.write_end
        result.complete = result_validator.complete
        
        result.type= RequestTypes.PLAYWRIGHT

        return result



