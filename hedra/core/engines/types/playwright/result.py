from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.engines.types.common.types import RequestTypes
from .command import PlaywrightCommand


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
            command.hooks.checks,
            error
        )

        self.url = command.url.location
        self.headers = command.url.headers
        self.command = command.command
        self.selector = command.page.selector
        self.x_coord = command.page.x_coordinate
        self.y_coord = command.page.y_coordinate
        self.frame = command.page.frame