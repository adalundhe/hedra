from hedra.core.engines.types.playwright import PlaywrightResult
from .base_event import BaseEvent


class PlaywrightEvent(BaseEvent):

    __slots__ = (
        'event_id',
        'action_id',
        'url',
        'headers',
        'command',
        'selector',
        'x_coord',
        'y_coord',
        'frame',
        'timings'
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

        self.time = result.complete - result.start

        self.timings = {
            'total': self.time,
            'waiting': result.start - result.wait_start,
            'connecting': result.connect_end - result.start,
            'writing': result.write_end - result.connect_end,
            'reading': result.complete - result.write_end
        }