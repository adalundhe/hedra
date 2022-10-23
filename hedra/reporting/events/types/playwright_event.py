from hedra.core.engines.types.playwright import PlaywrightResult
from .base_event import BaseEvent


class PlaywrightEvent(BaseEvent):

    __slots__ = (
        'url',
        'headers',
        'command',
        'selector',
        'x_coord',
        'y_coord',
        'frame'
    )

    def __init__(self, result: PlaywrightResult) -> None:
        super(
            PlaywrightEvent,
            self
        ).__init__(result)

        self.url = result.url
        self.headers = result.headers
        self.command = result.command
        self.selector = result.selector
        self.x_coord = result.x_coord
        self.y_coord = result.y_coord
        self.frame = result.frame