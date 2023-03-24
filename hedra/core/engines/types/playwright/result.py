from typing import Dict, Union
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.engines.types.common.types import RequestTypes
from .command import PlaywrightCommand, Page, URL


class PlaywrightResult(BaseResult):

    __slots__ = (
        'action_id',
        'url',
        'headers',
        'command',
        'selector',
        'x_coord',
        'y_coord',
        'frame'
    )

    def __init__(self, command: PlaywrightCommand, error: Exception=None) -> None:
        super(
            PlaywrightResult,
            self
        ).__init__(
            command.action_id,
            command.name,
            command.url.location,
            command.metadata.user,
            command.metadata.tags,
            RequestTypes.PLAYWRIGHT,
            error
        )

        self.url = command.url.location
        self.headers = command.url.headers
        self.command = command.command
        self.selector = command.page.selector
        self.x_coord = command.page.x_coordinate
        self.y_coord = command.page.y_coordinate
        self.frame = command.page.frame

    def to_dict(self):

        encoded_headers = dict(self.headers)

        base_result_dict = super().to_dict()
        
        return {
            'url': self.url,
            'command': self.command,
            'selector': self.selector,
            'x_coord': self.x_coord,
            'y_coord': self.y_coord,
            'frame': self.frame,
            'type': self.type,
            'headers': encoded_headers,
            'tags': self.tags,
            'user': self.user,
            'error': str(self.error),
            **base_result_dict
        }

    @classmethod
    def from_dict(cls, results_dict: Dict[str, Union[int, float, str,]]):

        playwright_command = PlaywrightCommand(
            results_dict.get('name'),
            results_dict.get('command'),
            page=Page(
                selector=results_dict.get('selector'),
                x_coordinate=results_dict.get('x_coord'),
                y_coordinate=results_dict.get('y_coord'),
                frame=results_dict.get('frame')
            ),
            url=URL(
                location=results_dict.get('url'),
                headers=results_dict.get('headers')
            ),
            user=results_dict.get('user'),
            tags=results_dict.get('tags')
        )

     
        playwright_result = PlaywrightResult(
            playwright_command,
            error=results_dict.get('error')
        )

        playwright_result.checks = results_dict.get('checks')
        playwright_result.source = results_dict.get('source')
        playwright_result.wait_start = results_dict.get('wait_start')
        playwright_result.start = results_dict.get('start')
        playwright_result.connect_end = results_dict.get('connect_end')
        playwright_result.write_end = results_dict.get('write_end')
        playwright_result.complete = results_dict.get('complete')

        return playwright_result