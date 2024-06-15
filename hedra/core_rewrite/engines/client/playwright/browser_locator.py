from __future__ import annotations

import asyncio
import time
from typing import Dict, List, Literal, Optional

from playwright.async_api import (
    FloatRect,
    Locator,
)

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .models.browser import BrowserMetadata
from .models.commands.locator import (
    AllTextsCommand,
    AndMatchingCommand,
    BoundingBoxCommand,
)
from .models.results import PlaywrightResult


class BrowserLocator:

    def __init__(
        self,
        locator: Locator,
        timeouts: Timeouts,
        metadata: BrowserMetadata,
        url: str
    ) -> None:
        self.locator = locator
        self.timeouts = timeouts
        self.metadata = metadata
        self.url = url

    async def all(self):
        return [
            BrowserLocator(
                locator,
                self.timeouts,
                self.metadata,
                self.url
            ) for locator in (
                await self.locator.all()
            )
        ]
    
    async def all_inner_texts(
        self,
        timeout: Optional[int | float]=None
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


        command = AllTextsCommand(
            timeout=timeout
        )
        
        result: List[str] = None
        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.locator.all_inner_texts(),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='all_inner_texts',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='all_inner_texts',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
    
    async def all_text_contents(
        self,
        timeout: Optional[int | float]=None
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


        command = AllTextsCommand(
            timeout=timeout
        )
        
        result: List[str] = None
        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.locator.all_text_contents(),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='all_text_contents',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='all_text_contents',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
    
    async def and_matching(
        self,
        locator: BrowserLocator,
        timeout: Optional[int | float]=None
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


        command = AndMatchingCommand(
            locator=locator.locator,
            timeout=timeout
        )
        
        result: List[str] = None
        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.locator.and_(command.locator),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='and_matching',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='and_matching',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
    
    async def blur(
        self,
        timeout: Optional[int | float]=None
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


        command = BoundingBoxCommand(
            timeout=timeout
        )
        
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.blur(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='blur',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='blur',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def bounding_box(
        self,
        timeout: Optional[int | float]=None
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


        command = BoundingBoxCommand(
            timeout=timeout
        )
        
        result: FloatRect = None
        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:

            result = await self.locator.bounding_box(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='bounding_box',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='bounding_box',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )