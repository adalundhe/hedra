import asyncio
import time
from typing import (
    Dict,
    Literal,
    Optional,
)

from playwright.async_api import Touchscreen

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .models.browser import BrowserMetadata
from .models.commands.touchscreen import TapCommand
from .models.results import PlaywrightResult


class BrowserTouchscreen:
    def __init__(
        self,
        touchscreen: Touchscreen,
        timeouts: Timeouts,
        metadata: BrowserMetadata,
        url: str,
    ) -> None:
        self.touchscreen = touchscreen
        self.timeouts = timeouts
        self.metadata = metadata
        self.url = url

    async def tap(
        self,
        x_position: int | float,
        y_position: int | float,
        timeout: Optional[int | float] = None,
    ):
        if timeout is None:
            timeout = self.timeouts.request_timeout * 1000

        timings: Dict[Literal["command_start", "command_end"], float] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout * 1000

        command = TapCommand(
            x_position=x_position,
            y_position=y_position,
            timeout=timeout,
        )

        err: Optional[Exception] = None
        timings["command_start"] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.touchscreen.tap(
                    x=command.x_position,
                    y=command.y_position,
                ),
                timeout=command.timeout,
            )

        except Exception as err:
            timings["command_end"] = time.monotonic()

            return PlaywrightResult(
                command="tap",
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
            )

        timings["command_end"] = time.monotonic()

        return PlaywrightResult(
            command="tap",
            command_args=command,
            result=None,
            metadata=self.metadata,
            timings=timings,
            url=self.url,
        )
