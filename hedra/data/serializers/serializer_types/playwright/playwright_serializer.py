from hedra.core.engines.types.playwright.command import (
    PlaywrightCommand,
    Page,
    Input,
    Options,
    URL
)
from hedra.core.engines.types.common.types import RequestTypes
from hedra.data.serializers.serializer_types.common.base_serializer import BaseSerializer
from typing import List, Dict, Union, Any


class PlaywrightSerializer(BaseSerializer):

    def __init__(self) -> None:
        super().__init__()

    def serialize_action(
        self,
        action: PlaywrightCommand
    ) -> Dict[str, Union[str, List[str]]]:
        
        serialized_action = super().serialize_action()
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
        
        http_action = PlaywrightCommand(
            name=action.get('name'),
            url=url_config.get('full'),
            headers=action.get('headers'),
            data=action.get('data'),
            user=metadata.get('user'),
            tags=metadata.get('tags', []),
            redirects=action.get('redirects', 3)
        )

        http_action.url.ip_addr = url_config.get('ip_addr')
        http_action.url.socket_config = url_config.get('socket_config')
        http_action.url.has_ip_addr = url_config.get('has_ip_addr')

        http_action.setup()

        return http_action
    