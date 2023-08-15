from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.playwright.command import (
    PlaywrightCommand,
    Page,
    Input,
    Options,
    URL
)
from hedra.core.engines.types.playwright.client import (
    MercuryPlaywrightClient, 
    ContextConfig
)
from hedra.core.engines.types.playwright.result import PlaywrightResult
from hedra.core.engines.types.common.types import RequestTypes
from hedra.data.serializers.serializer_types.common.base_serializer import BaseSerializer
from typing import List, Dict, Union, Any


class PlaywrightSerializer(BaseSerializer):

    def __init__(self) -> None:
        super().__init__()

    def action_to_serializable(
        self,
        action: PlaywrightCommand
    ) -> Dict[str, Union[str, List[str]]]:
        
        serialized_action = super().action_to_serializable(action)
        return {
            **serialized_action,
            'type': RequestTypes.PLAYWRIGHT,
            'command': action.command,
            'command_args': action.command_args,
            'url': {
                'location': action.url.location,
                'headers': action.url.headers,
            },
            'page': {
                'selector': action.page.selector,
                'attribute': action.page.attribute,
                'x_coordinate': action.page.x_coordinate,
                'y_coordinate': action.page.y_coordinate,
                'frame': action.page.frame
            },
            'input': {
                'key': action.input.key,
                'text': action.input.text,
                'expression': action.input.expression,
                'args': action.input.args,
                'filepath': action.input.filepath,
                'file': action.input.file,
                'path': action.input.path,
                'option': action.input.option,
                'by_label': action.input.by_label,
                'by_value': action.input.by_value
            },
            'options': {
                'event': action.options.event,
                'option': action.options.option,
                'is_checked': action.options.is_checked,
                'timeout': action.options.timeout,
                'extra': action.options.extra,
                'switch_by': action.options.switch_by
            }
        }
    
    def deserialize_action(
        self,
        action: Dict[str, Any]
    ) -> PlaywrightCommand:
        
        url_config = action.get('url', {})
        metadata = action.get('metadata', {})
        page_config = action.get('page', {})
        input_config = action.get('input', {})
        url_config = action.get('url', {})
        options_config=action.get('options', {})
        
        playwright_command = PlaywrightCommand(
            name=action.get('name'),
            command=action.get('command'),
            page=Page(
                selector=page_config.get('selector'),
                attribute=page_config.get('attribute'),
                x_coordinate=page_config.get('x_coordinate'),
                y_coordinate=page_config.get('y_coordinate'),
                frame=page_config.get('frame', 0)
            ),
            url=URL(
                location=url_config.get('locations'),
                headers=url_config.get('headers', {})
            ),
            input=Input(
                key=input_config.get('key'),
                text=input_config.get('text'),
                expression=input_config.get('expression'),
                args=input_config.get('args'),
                filepath=input_config.get('filepath'),
                file=input_config.get('file'),
                path=input_config.get('path'),
                option=input_config.get('option'),
                by_label=input_config.get('by_label', False),
                by_value=input_config.get('by_value', False)
            ),
            options=Options(
                event=options_config.get('event'),
                option=options_config.get('option'),
                is_checked=options_config.get('is_checked', False),
                timeout=options_config.get('timeout', 10),
                extra=options_config.get('extra', {}),
                switch_by=options_config.get('switch_by', 'url')
            ),
            user=metadata.get('user'),
            tags=metadata.get('tags', [])
        )

        return playwright_command
    
    def deserialize_client_config(self, client_config: Dict[str, Any]) -> MercuryPlaywrightClient:
        playwright_client = MercuryPlaywrightClient(
            concurrency=client_config.get('concurrency'),
            group_size=client_config.get('group_size'),
            timeouts=Timeouts(
                **client_config.get('timeouts', {})
            )
        )

        playwright_client.config = ContextConfig(
            **client_config.get('context_config')
        )
        
        return playwright_client
    
    def result_to_serializable(
        self,
        result: PlaywrightResult
    ) -> Dict[str, Any]:
        
        serialized_result = super().result_to_serializable(result)

        encoded_headers = dict(result.headers)

        return {
            **serialized_result,
            'url': result.url,
            'command': result.command,
            'selector': result.selector,
            'attribute': result.attribute,
            'x_coord': result.x_coord,
            'y_coord': result.y_coord,
            'frame': result.frame,
            'type': result.type,
            'headers': encoded_headers,
            'tags': result.tags,
            'user': result.user,
            'key': result.key,
            'text': result.text,
            'expression': result.expression,
            'args': result.args,
            'filepath': result.filepath,
            'file': result.file,
            'option': result.option,
            'event': result.event,
            'timeout': result.option,
            'is_checked': result.is_checked,
            'error': str(result.error)
        }
    
    def deserialize_result(
        self,
        result: Dict[str, Any]
    ) -> PlaywrightResult:
        
        playwright_command = PlaywrightCommand(
            result.get('name'),
            result.get('command'),
            page=Page(
                selector=result.get('selector'),
                x_coordinate=result.get('x_coord'),
                y_coordinate=result.get('y_coord'),
                frame=result.get('frame')
            ),
            url=URL(
                location=result.get('url'),
                headers=result.get('headers')
            ),
            input=Input(
                key=result.get('key'),
                text=result.get('text'),
                expression=result.get('expression'),
                args=result.get('args'),
                filepath=result.get('filepath'),
                file=result.get('file'),
                path=result.get('path'),
                option=result.get('option'),
                by_label=result.get('by_label'),
                by_value=result.get('by_value')
            ),
            options=Options(
                event=result.get('event'),
                is_checked=result.get('is_checked'),
                timeout=result.get('timeout')
            ),
            user=result.get('user'),
            tags=result.get('tags')
        )

     
        playwright_result = PlaywrightResult(
            playwright_command,
            error=result.get('error')
        )

        playwright_result.checks = result.get('checks')
        playwright_result.wait_start = result.get('wait_start')
        playwright_result.start = result.get('start')
        playwright_result.connect_end = result.get('connect_end')
        playwright_result.write_end = result.get('write_end')
        playwright_result.complete = result.get('complete')
        
        playwright_result.type= RequestTypes.PLAYWRIGHT

        return playwright_result
    