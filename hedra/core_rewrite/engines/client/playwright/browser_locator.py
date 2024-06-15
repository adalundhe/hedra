from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import (
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Pattern,
    Sequence,
)

from playwright.async_api import (
    ElementHandle,
    FilePayload,
    FloatRect,
    FrameLocator,
    JSHandle,
    Locator,
    Position,
)

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .models.browser import BrowserMetadata
from .models.commands.locator import (
    AllTextsCommand,
    AndMatchingCommand,
    BoundingBoxCommand,
    CheckCommand,
    ClickCommand,
    CountCommand,
    DispatchEventCommand,
    DOMCommand,
    DragToCommand,
    FillCommand,
    FilterCommand,
    FocusCommand,
    GetAttributeCommand,
    HighlightCommand,
    HoverCommand,
    NthCommand,
    OrMatchingCommand,
    PressCommand,
    PressSequentiallyCommand,
    ScrollIntoViewIfNeeded,
    SelectOptionCommand,
    SelectTextCommand,
    SetCheckedCommand,
    SetInputFilesCommand,
    TapCommand,
    WaitForCommand,
)
from .models.commands.page import (
    EvaluateCommand,
    FrameLocatorCommand,
    GetByRoleCommand,
    GetByTestIdCommand,
    GetByTextCommand,
    LocatorCommand,
    ScreenshotCommand,
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

    def filter(
        self,
        has: Optional[BrowserLocator]=None,
        has_not: Optional[BrowserLocator]=None,
        has_text: Optional[str | Pattern]=None,
        has_not_text: Optional[str | Pattern]=None
    ):

        command = FilterCommand(
            has=has,
            has_not=has_not,
            has_text=has_text,
            has_not_text=has_not_text
        )
        return BrowserLocator(
            self.locator.filter(
                has=command.has,
                has_not=command.has_not,
                has_text=command.has_text,
                has_not_text=command.has_not_text
            ),
            self.timeouts,
            self.metadata,
            self.url
        )
    
    def nth(
        self,
        index: int,
    ):
        
        command = NthCommand(
            index=index
        )

        return BrowserLocator(
            self.locator.nth(command.index),
            self.timeouts,
            self.metadata,
            self.url
        )

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
    
    async def check(
        self,
        postion: Optional[Position]=None,
        force: Optional[bool]=None,
        no_wait_after: Optional[bool]=None,
        trial: Optional[bool]=None,
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

        command = CheckCommand(
            postion=postion,
            no_wait_after=no_wait_after,
            force=force,
            trial=trial,
            timeout=timeout,
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.check(
                position=command.postion,
                force=command.force,
                no_wait_after=command.no_wait_after,
                trial=command.trial,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='check',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='check',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def clear(
        self,
        timeout: Optional[int | float]=None,
        force: Optional[bool]=None,
        no_wait_after: Optional[bool]=None
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

        command = CheckCommand(
            no_wait_after=no_wait_after,
            force=force,
            timeout=timeout,
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.clear(
                force=command.force,
                no_wait_after=command.no_wait_after,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='clear',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='clear',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )

    async def click(
        self,
        modifiers: Optional[Sequence[Literal['Alt', 'Control', 'ControlOrMeta', 'Meta', 'Shift']]]=None,
        delay: Optional[int | float]=None,
        button: Optional[Literal['left', 'middle', 'right']]=None,
        click_count: Optional[int]=None,
        postion: Optional[Position]=None,
        timeout: Optional[int | float]=None,
        force: Optional[bool]=None,
        no_wait_after: Optional[bool]=None,
        trial: Optional[bool]=None
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
            modifiers=modifiers,
            delay=delay,
            button=button,
            click_count=click_count,
            postion=postion,
            no_wait_after=no_wait_after,
            force=force,
            trial=trial,
            timeout=timeout,
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:
        
            await self.locator.click(
                position=command.postion,
                modifiers=command.modifiers,
                delay=command.delay,
                button=command.button,
                click_count=command.click_count,
                force=command.force,
                no_wait_after=command.no_wait_after,
                trial=command.trial,
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
    
    async def count(
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


        command = CountCommand(
            timeout=timeout
        )
        
        result: int = None
        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.locator.count(),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='count',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='count',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )

    async def double_click(
        self,
        modifiers: Optional[Sequence[Literal['Alt', 'Control', 'ControlOrMeta', 'Meta', 'Shift']]]=None,
        delay: Optional[int | float]=None,
        button: Optional[Literal['left', 'middle', 'right']]=None,
        postion: Optional[Position]=None,
        force: Optional[bool]=None,
        no_wait_after: Optional[bool]=None,
        trial: Optional[bool]=None,
        timeout: Optional[int | float] = None
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
            modifiers=modifiers,
            delay=delay,
            button=button,
            postion=postion,
            no_wait_after=no_wait_after,
            force=force,
            trial=trial,
            timeout=timeout,
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.dblclick(
                position=command.postion,
                modifiers=command.modifiers,
                delay=command.delay,
                button=command.button,
                force=command.force,
                no_wait_after=command.no_wait_after,
                trial=command.trial,
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
    
    async def dispatch_event(
        self,
        event_type: str,
        event_init: Optional[Dict[str, Any]] = None,
        timeout: Optional[int | float] = None
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

        command = DispatchEventCommand(
            event_type=event_type,
            event_init=event_init,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.dispatch_event(
                type=command.event_type,
                event_init=command.event_init,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='dispatch_event',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='dispatch_event',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def drag_to(
        self,
        target: BrowserLocator,
        force: Optional[bool] = None,
        no_wait_after: Optional[bool] = None,
        trial: Optional[bool] = None,
        source_position: Optional[Position] = None,
        target_position: Optional[Position] = None,
        timeout: Optional[int | float] =None
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

        command = DragToCommand(
            target=target,
            force=force,
            no_wait_after=no_wait_after,
            trial=trial,
            source_position=source_position,
            target_position=target_position,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await asyncio.wait_for(
                self.locator.drag_to(
                    command.target,
                    force=command.force,
                    no_wait_after=command.no_wait_after,
                    trial=command.trial,
                    source_position=command.source_position,
                    target_position=command.target_position
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='drag_to',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='drag_to',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def evaluate(
        self,
        expression: str,
        arg: Any,
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

        command = EvaluateCommand(
            expression=expression,
            arg=arg,
            timeout=timeout
        )

        result: Any = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.locator.evaluate(
                    command.expression,
                    arg=command.arg,
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='evaluate',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='evaluate',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )
    
    async def evaluate_all(
        self,
        expression: str,
        arg: Any,
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

        command = EvaluateCommand(
            expression=expression,
            arg=arg,
            timeout=timeout
        )

        result: Any = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.locator.evaluate_all(
                    command.expression,
                    arg=command.arg,
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='evaluate_all',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='evaluate_all',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )

    async def evaluate_handle(
        self,
        expression: str,
        arg: Any,
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

        command = EvaluateCommand(
            expression=expression,
            arg=arg,
            timeout=timeout
        )

        result: JSHandle = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.locator.evaluate_handle(
                    command.expression,
                    arg=command.arg,
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='evaluate_handle',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='evaluate_handle',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )
    
    async def fill(
        self,
        value: str,
        no_wait_after: Optional[bool]=None,
        force: Optional[bool]=None,
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

        command = FillCommand(
            value=value,
            no_wait_after=no_wait_after,
            force=force,
            timeout=timeout,
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.fill(
                command.value,
                no_wait_after=command.no_wait_after,
                force=command.force,
                timeout=command.timeout,
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='fill',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='fill',
            command_args=command,
            result=None,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )
    
    async def focus(
        self,
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

        command = FocusCommand(
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.focus(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='focus',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='focus',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def frame_locator(
        self,
        selector: str,
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

        command = FrameLocatorCommand(
            selector=selector,
            timeout=timeout
        )

        result: FrameLocator = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.locator.frame_locator(
                    command.selector
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='frame_locator',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='frame_locator',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
    
    async def get_by_alt_text(
        self,
        text: str | Pattern[str],
        exact: Optional[bool] = None,
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

        command = GetByTextCommand(
            text=text,
            exact=exact,
            timeout=timeout
        )

        result: Locator = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.locator.get_by_alt_text(
                    command.text,
                    exact=command.exact
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='get_by_alt_text',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='get_by_alt_text',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings
        )
    
    async def get_by_label(
        self,
        text: str | Pattern[str],
        exact: Optional[bool] = None,
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

        command = GetByTextCommand(
            text=text,
            exact=exact,
            timeout=timeout
        )

        result: Locator = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.locator.get_by_label(
                    command.text,
                    exact=command.exact
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='get_by_label',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='get_by_label',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
    
    async def get_by_placeholder(
        self,
        text: str | Pattern[str],
        exact: Optional[bool] = None,
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

        command = GetByTextCommand(
            text=text,
            exact=exact,
            timeout=timeout
        )

        result: Locator = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.locator.get_by_placeholder(
                    command.text,
                    exact=command.exact
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='get_by_placeholder',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='get_by_placeholder',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings
        )
    
    async def get_by_role(
        self,
        role: Literal['alert', 'alertdialog', 'application', 'article', 'banner', 'blockquote', 'button', 'caption', 'cell', 'checkbox', 'code', 'columnheader', 'combobox', 'complementary', 'contentinfo', 'definition', 'deletion', 'dialog', 'directory', 'document', 'emphasis', 'feed', 'figure', 'form', 'generic', 'grid', 'gridcell', 'group', 'heading', 'img', 'insertion', 'link', 'list', 'listbox', 'listitem', 'log', 'main', 'marquee', 'math', 'menu', 'menubar', 'menuitem', 'menuitemcheckbox', 'menuitemradio', 'meter', 'navigation', 'none', 'note', 'option', 'paragraph', 'presentation', 'progressbar', 'radio', 'radiogroup', 'region', 'row', 'rowgroup', 'rowheader', 'scrollbar', 'search', 'searchbox', 'separator', 'slider', 'spinbutton', 'status', 'strong', 'subscript', 'superscript', 'switch', 'tab', 'table', 'tablist', 'tabpanel', 'term', 'textbox', 'time', 'timer', 'toolbar', 'tooltip', 'tree', 'treegrid', 'treeitem'],
        checked: Optional[bool] = None,
        disabled: Optional[bool] = None,
        expanded: Optional[bool] = None,
        include_hidden: Optional[bool] = None,
        level: Optional[int] = None,
        name: Optional[str | Pattern[str]] = None,
        pressed: Optional[bool] = None,
        selected: Optional[bool] = None,
        exact: Optional[bool] = None,
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

        command = GetByRoleCommand(
            role=role,
            checked=checked,
            disabled=disabled,
            expanded=expanded,
            include_hidden=include_hidden,
            level=level,
            name=name,
            pressed=pressed,
            selected=selected,
            exact=exact,
            timeout=timeout
        )

        result: Locator = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.locator.get_by_role(
                    command.role,
                    checked=command.checked,
                    disabled=command.disabled,
                    expanded=command.expanded,
                    include_hidden=command.include_hidden,
                    level=command.level,
                    name=command.name,
                    pressed=command.pressed,
                    selected=command.selected,
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='get_by_role',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='get_by_role',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
        
    async def get_by_test_id(
        self,
        test_id: str | Pattern[str],
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

        command = GetByTestIdCommand(
            test_id=test_id,
            timeout=timeout
        )

        result: Locator = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.locator.get_by_test_id(
                    command.test_id
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='get_by_test_id',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='get_by_test_id',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings
        )

    async def get_by_text(
        self,
        text: str | Pattern[str],
        exact: Optional[bool] = None,
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

        command = GetByTextCommand(
            text=text,
            exact=exact,
            timeout=timeout
        )

        result: Locator = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.locator.get_by_text(
                    command.text,
                    exact=command.exact
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='get_by_text',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='get_by_text',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings
        )

    async def get_by_title(
        self,
        text: str | Pattern[str],
        exact: Optional[bool] = None,
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

        command = GetByTextCommand(
            text=text,
            exact=exact,
            timeout=timeout
        )

        result: Locator = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.locator.get_by_title(
                    command.text,
                    exact=command.exact
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='get_by_title',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='get_by_title',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
    
    async def get_attribute(
        self,
        name: str,
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

        command = GetAttributeCommand(
            name=name,
            timeout=timeout,
        )

        result: Optional[str] = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.locator.get_attribute(
                command.name,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='get_attribute',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='get_attribute',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
    
    async def highlight(
        self,
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

        command = HighlightCommand(
            timeout=timeout,
        )

        result: Optional[str] = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.locator.highlight(),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='highlight',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='highlight',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
    
    async def hover(
        self,
        modifiers: Optional[Sequence[Literal['Alt', 'Control', 'ControlOrMeta', 'Meta', 'Shift']]]=None,
        postion: Optional[Position]=None,
        force: Optional[bool]=None,
        no_wait_after: Optional[bool]=None,
        trial: Optional[bool]=None,
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

        command = HoverCommand(
            modifiers=modifiers,
            postion=postion,
            force=force,
            no_wait_after=no_wait_after,
            trial=trial,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.hover(
                modifiers=command.modifiers,
                position=command.postion,
                force=command.force,
                no_wait_after=command.no_wait_after,
                trial=command.trial,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='hover',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='hover',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def inner_html(
        self,
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

        command = DOMCommand(
            timeout=timeout
        )

        result: str = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.locator.inner_html(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='inner_html',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='inner_html',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )

    async def inner_text(
        self,
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

        command = DOMCommand(
            timeout=timeout
        )

        result: str = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.locator.inner_text(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='inner_text',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='inner_text',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )
    
    async def input_value(
        self,
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

        command = DOMCommand(
            timeout=timeout
        )

        result: str = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.locator.input_value(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='input_value',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='input_value',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )
    
    async def is_enabled(
        self,
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

        command = DOMCommand(
            timeout=timeout
        )

        result: bool = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.locator.is_enabled(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='is_enabled',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='is_enabled',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )

    async def is_hidden(
        self,
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

        command = DOMCommand(
            timeout=timeout
        )

        result: bool = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.locator.is_hidden(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='is_hidden',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='is_hidden',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )

    async def is_visible(
        self,
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

        command = DOMCommand(
            timeout=timeout
        )

        result: bool = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.locator.is_visible(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='is_visible',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='is_visible',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )

    async def is_checked(
        self,
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

        command = DOMCommand(
            timeout=timeout
        )

        result: bool = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.locator.is_checked(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='is_checked',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='is_checked',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )
    
    async def is_disabled(
        self,
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

        command = DOMCommand(
            timeout=timeout
        )

        result: bool = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await self.locator.is_disabled(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='is_disabled',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='is_disabled',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings
        )
    
    async def is_editable(
        self,
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

        command = DOMCommand(
            timeout=timeout
        )

        result: bool = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await self.locator.is_editable(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='is_editable',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='is_editable',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings
        )
    
    async def locate(
        self,
        selector: str,
        has_text: Optional[str | Pattern[str]] = None,
        has_not_text: Optional[str | Pattern[str]] = None,
        has: Optional[Locator] = None,
        has_not: Optional[Locator] = None,
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

        command = LocatorCommand(
            selector=selector,
            has=has,
            has_not=has_not,
            has_text=has_text,
            has_not_text=has_not_text,
            timeout=timeout
        )

        result: Locator = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.locator.locator(
                    command.selector,
                    has_text=command.has_text,
                    has_not_text=command.has_not_text,
                    has=command.has,
                    has_not=command.has_not
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='locator',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='locator',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )
    
    async def or_matching(
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


        command = OrMatchingCommand(
            locator=locator.locator,
            timeout=timeout
        )
        
        result: List[str] = None
        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.locator.or_(command.locator),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='or_matching',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='or_matching',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
    
    async def press(
        self,
        key: str,
        delay: Optional[int | float] = None,
        no_wait_after: Optional[bool] = None,
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

        command = PressCommand(
            key=key,
            delay=delay,
            no_wait_after=no_wait_after,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.press(
                command.key,
                delay=command.delay,
                no_wait_after=command.no_wait_after,
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
    
    async def press_sequentially(
        self,
        text: str,
        delay: Optional[int | float] = None,
        no_wait_after: Optional[bool] = None,
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

        command = PressSequentiallyCommand(
            text=text,
            delay=delay,
            no_wait_after=no_wait_after,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.press_sequentially(
                command.key,
                delay=command.delay,
                no_wait_after=command.no_wait_after,
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
    
    async def screenshot(
        self,
        path: str | Path,
        image_type: Optional[Literal['jpeg', 'png']] = None,
        quality: Optional[int]= None,
        omit_background: Optional[bool] = None,
        full_page: Optional[bool] = None,
        clip: Optional[FloatRect] = None,
        animations: Optional[Literal['allow', 'disabled']] = None,
        caret: Optional[Literal['hide', 'initial']] = None,
        scale: Optional[Literal['css', 'device']] = None,
        mask: Optional[Sequence[Locator]] = None,
        mask_color: Optional[str] = None,
        style: Optional[str] = None,
        timeout: Optional[int | float] = None,
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

        command = ScreenshotCommand(
            path=path,
            image_type=image_type,
            quality=quality,
            omit_background=omit_background,
            full_page=full_page,
            clip=clip,
            animations=animations,
            caret=caret,
            scale=scale,
            mask=mask,
            mask_color=mask_color,
            style=style,
            timeout=timeout
        )
    
        result: bytes = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.locator.screenshot(
                path=command.path,
                type=command.image_type,
                quality=command.quality,
                omit_background=command.omit_background,
                full_page=command.full_page,
                clip=command.clip,
                animations=command.animations,
                caret=command.caret,
                scale=command.scale,
                mask=command.mask,
                mask_color=command.mask_color,
                style=command.style,
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='screenshot',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='screenshot',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )
    
    async def scroll_into_view_if_needed(
        self,
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

        command = ScrollIntoViewIfNeeded(
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.scroll_into_view_if_needed(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='scroll_into_view_if_needed',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='scroll_into_view_if_needed',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def select_option(
        self,
        value: Optional[str | Sequence[str]] = None,
        index: Optional[int | Sequence[int]] = None,
        label: Optional[str | Sequence[str]] = None,
        element: Optional[ElementHandle | Sequence[ElementHandle]] = None,
        no_wait_after: Optional[bool] = None,
        force: Optional[bool] = None,
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
    
        command = SelectOptionCommand(
            value=value,
            index=index,
            label=label,
            element=element,
            no_wait_after=no_wait_after,
            force=force,
            timeout=timeout
        )
    
        result: List[str] = []
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.select_option(
                value=command.value,
                index=command.index,
                label=command.label,
                element=command.element,
                no_wait_after=command.no_wait_after,
                force=command.force,
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='select_option',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='select_option',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )
    
    async def select_text(
        self,
        force: Optional[bool] = None,
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
    
        command = SelectTextCommand(
            force=force,
            timeout=timeout
        )
    
        result: List[str] = []
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.select_text(
                force=command.force,
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='select_text',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='select_text',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )

    async def set_checked(
        self,
        checked: bool,
        position: Optional[Position] = None,
        force: Optional[bool] = None,
        no_wait_after: Optional[bool] = None,
        trial: Optional[bool] = None,
        timeout: Optional[int | float] = None,
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
    
        command = SetCheckedCommand(
            checked=checked,
            position=position,
            force=force,
            no_wait_after=no_wait_after,
            trial=trial,
            timeout=timeout
        )
    
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.set_checked(
                command.checked,
                position=command.position,
                force=command.force,
                no_wait_after=command.no_wait_after,
                trial=command.trial,
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='set_checked',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='set_checked',
            command_args=command,
            result=None,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )
    
    async def set_input_files(
        self,
        files: str | Path | FilePayload | Sequence[str | Path] | Sequence[FilePayload],
        no_wait_after: Optional[bool] = None,
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

        command = SetInputFilesCommand(
            files=files,
            no_wait_after=no_wait_after,
            timeout=timeout
        )

        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            await self.locator.set_input_files(
                command.files,
                no_wait_after=command.no_wait_after,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='set_content',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='set_content',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def tap(
        self,
        modifiers: Optional[
            Sequence[
                Literal[
                    'Alt', 
                    'Control', 
                    'ControlOrMeta', 
                    'Meta', 
                    'Shift'
                ]
            ]
        ] = None,
        position: Optional[Position] = None,
        force: Optional[bool] = None,
        no_wait_after: Optional[bool] = None,
        trial: Optional[bool] = None,
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
    
        command = TapCommand(
            modifiers=modifiers,
            position=position,
            force=force,
            no_wait_after=no_wait_after,
            trial=trial,
            timeout=timeout
        )
    
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.tap(
                modifiers=command.modifiers,
                position=command.position,
                force=command.force,
                no_wait_after=command.no_wait_after,
                trial=command.trial,
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='tap',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='tap',
            command_args=command,
            result=None,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )
    
    async def text_content(
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
    
        command = DOMCommand(
            timeout=timeout
        )

        result: Optional[str]=None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.locator.text_content(
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='text_content',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='text_content',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
        )
    
    async def uncheck(
        self,
        postion: Optional[Position]=None,
        timeout: Optional[int | float]=None,
        force: Optional[bool]=None,
        no_wait_after: Optional[bool]=None,
        trial: Optional[bool]=None
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

        command = CheckCommand(
            postion=postion,
            no_wait_after=no_wait_after,
            force=force,
            trial=trial,
            timeout=timeout,
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.uncheck(
                position=command.postion,
                force=command.force,
                no_wait_after=command.no_wait_after,
                trial=command.trial,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='uncheck',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='uncheck',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def wait_for(
        self,
        state: Literal['attached', 'detached', 'visible', 'hidden']='visible',
        timeout: Optional[int | float]=None,
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

        command = WaitForCommand(
            state=state,
            timeout=timeout,
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.locator.wait_for(
                state=command.state,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='wait_for',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='wait_for',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )