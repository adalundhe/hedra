from __future__ import annotations
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
        'attribute',
        'x_coord',
        'y_coord',
        'frame',
        'key',
        'text',
        'expression',
        'args',
        'filepath',
        'file',
        'option',
        'event',
        'is_checked'
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
        self.attribute = command.page.attribute
        self.x_coord = command.page.x_coordinate
        self.y_coord = command.page.y_coordinate
        self.frame = command.page.frame
        self.key = command.input.key
        self.text = command.input.text
        self.expression = command.input.expression
        self.args = command.input.args
        self.filepath = command.input.filepath
        self.file = command.input.file
        self.option = command.input.option
        self.event = command.options.event
        self.timeout = command.options.timeout
        self.is_checked = command.options.is_checked
