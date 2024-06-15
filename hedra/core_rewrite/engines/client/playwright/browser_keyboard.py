import asyncio
import time
from typing import Dict, Literal, Optional

from playwright.async_api import Keyboard

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .models.browser import BrowserMetadata
from .models.commands.keyboard import (
    KeyCommand,
    PressCommand,
    TypeCommand,
)
from .models.results import PlaywrightResult


class BrowserKeyboard:

    def __init__(
        self,
        keyboard: Keyboard,
        timeouts: Timeouts,
        metadata: BrowserMetadata,
        url: str
    ) -> None:
        self.keyboard = keyboard
        self.timeouts = timeouts
        self.metadata = metadata
        self.url = url

    async def down(
        self,
        key: str,
        timeout: Optional[int | float]=None
    ):
        
        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = KeyCommand(
            key=key,
            timeout=timeout,
        )

        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.keyboard.down(command.key),
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
        key: str,
        timeout: Optional[int | float]=None
    ):
        
        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = KeyCommand(
            key=key,
            timeout=timeout,
        )

        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.keyboard.up(command.key),
                timeout=command.timeout
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
    
    async def press(
        self,
        key: str,
        delay: Optional[int | float]=None,
        timeout: Optional[int | float]=None
    ):
        
        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = PressCommand(
            key=key,
            delay=delay,
            timeout=timeout,
        )

        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.keyboard.press(
                    command.key,
                    delay=command.delay
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='press',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='press',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def type_text(
        self,
        text: str,
        delay: Optional[int | float]=None,
        timeout: Optional[int | float]=None
    ):
        
        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = TypeCommand(
            text=text,
            delay=delay,
            timeout=timeout,
        )

        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.keyboard.type(
                    command.text,
                    delay=command.delay
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='type_text',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='type_text',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def insert_text(
        self,
        text: str,
        timeout: Optional[int | float]=None
    ):
        
        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = TypeCommand(
            text=text,
            timeout=timeout,
        )

        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.keyboard.insert_text(command.text),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='insert_text',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='insert_text',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )