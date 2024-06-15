import asyncio
import time
from typing import Dict, Literal, Optional

from playwright.async_api import (
    Mouse,
)

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .models.browser import BrowserMetadata
from .models.commands.mouse import (
    ButtonCommand,
    ClickCommand,
    MoveCommand,
    WheelCommand,
)
from .models.results import PlaywrightResult


class BrowserMouse:

    def __init__(
        self,
        mouse: Mouse,
        timeouts: Timeouts,
        metadata: BrowserMetadata,
        url: str
    ) -> None:
        self.mouse = mouse
        self.timeouts = timeouts
        self.metadata = metadata
        self.url = url

    async def click(
        self,
        x_position: int | float,
        y_position: int | float,
        delay: Optional[int | float]=None,
        button: Optional[Literal['left', 'middle', 'right']]='left',
        click_count: Optional[int]=None,
        timeout: Optional[int | float]=None,
    ):

        if timeout is None:
            timeout = self.timeouts.request_timeout

        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = ClickCommand(
            x_position=x_position,
            y_position=y_position,
            delay=delay,
            button=button,
            click_count=click_count,
            timeout=timeout
        )

        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
        
            await asyncio.wait_for(
                self.mouse.click(
                    x=command.x_position,
                    y=command.y_position,
                    delay=command.delay,
                    button=command.button,
                    click_count=command.click_count
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='click',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='click',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def double_click(
        self,
        x_position: int | float,
        y_position: int | float,
        delay: Optional[int | float]=None,
        button: Optional[Literal['left', 'middle', 'right']]='left',
        timeout: Optional[int | float]=None,
    ):

        if timeout is None:
            timeout = self.timeouts.request_timeout

        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = ClickCommand(
            x_position=x_position,
            y_position=y_position,
            delay=delay,
            button=button,
            timeout=timeout
        )

        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
        
            await asyncio.wait_for(
                self.mouse.dblclick(
                    x=command.x_position,
                    y=command.y_position,
                    delay=command.delay,
                    button=command.button
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='double_click',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='double_click',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def move(
        self,
        x_position: int | float,
        y_position: int | float,
        steps: int=1,
        timeout: Optional[int | float]=None,
    ):

        if timeout is None:
            timeout = self.timeouts.request_timeout

        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = MoveCommand(
            x_position=x_position,
            y_position=y_position,
            steps=steps,
            timeout=timeout
        )

        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
        
            await asyncio.wait_for(
                self.mouse.move(
                    x=command.x_position,
                    y=command.y_position,
                    steps=command.steps
                ),
                timeout=command.timeout
            )
        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='move',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='move',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def down(
        self,
        button: Optional[Literal['left', 'middle', 'right']]='left',
        click_count: Optional[int]=None,
        timeout: Optional[int | float]=None,
    ):

        if timeout is None:
            timeout = self.timeouts.request_timeout

        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = ButtonCommand(
            button=button,
            click_count=click_count,
            timeout=timeout
        )

        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
        
            await asyncio.wait_for(
                self.mouse.down(
                    button=command.button,
                    click_count=command.click_count
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='down',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='down',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def up(
        self,
        button: Optional[Literal['left', 'middle', 'right']]='left',
        click_count: Optional[int]=None,
        timeout: Optional[int | float]=None,
    ):

        if timeout is None:
            timeout = self.timeouts.request_timeout

        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = ButtonCommand(
            button=button,
            click_count=click_count,
            timeout=timeout
        )

        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
        
            await asyncio.wait_for(
                self.mouse.up(
                    button=command.button,
                    click_count=command.click_count
                ),
                command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='up',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='up',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def wheel(
        self,
        delta_x: int | float,
        delta_y: int | float,
        timeout: Optional[int | float]=None,
    ):

        if timeout is None:
            timeout = self.timeouts.request_timeout

        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = WheelCommand(
            delta_x=delta_x,
            delta_y=delta_y,
            timeout=timeout
        )

        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
        
            await asyncio.wait_for(
                self.mouse.wheel(
                    delta_x=command.delta_x,
                    delta_y=command.delta_y
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='wheel',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='wheel',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )