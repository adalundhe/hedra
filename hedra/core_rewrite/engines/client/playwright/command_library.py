
import asyncio
import time
from collections import deque
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Deque,
    Dict,
    List,
    Literal,
    Optional,
    Pattern,
    Sequence,
    Tuple,
)

from playwright._impl._async_base import AsyncEventContextManager
from playwright.async_api import (
    BrowserContext,
    ConsoleMessage,
    Download,
    ElementHandle,
    FileChooser,
    FloatRect,
    Frame,
    Geolocation,
    JSHandle,
    Locator,
    Page,
    Position,
    Request,
    Response,
    async_playwright,
)

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .browser_session import BrowserSession
from .models.commands import (
    BringToFrontCommand,
    CheckCommand,
    ClickCommand,
    ContentCommand,
    DispatchEventCommand,
    DOMCommand,
    DoubleClickCommand,
    DragAndDropCommand,
    EvaluateCommand,
    EvaluateOnSelectorCommand,
    ExpectConsoleMessageCommand,
    ExpectDownloadCommand,
    ExpectEventCommand,
    ExpectFileChooserCommand,
    ExpectNavigationCommand,
    ExpectPopupCommand,
    ExpectRequestCommand,
    ExpectRequestFinishedCommand,
    ExpectResponseCommand,
    FillCommand,
    FocusCommand,
    FrameCommand,
    GetUrlCommand,
    GoToCommand,
    HoverCommand,
    PressCommand,
    ReloadCommand,
    ScreenshotCommand,
    SelectOptionCommand,
    SetCheckedCommand,
    SetExtraHTTPHeadersCommand,
    SetTimeoutCommand,
    TapCommand,
    TitleCommand,
    TypeCommand,
    WaitForFunctionCommand,
    WaitForLoadStateCommand,
    WaitForSelectorCommand,
    WaitForTimeoutCommand,
    WaitForUrlCommand,
)
from .models.results import PlaywrightResult


class MercurySyncPlaywrightClient:


    def __init__(
        self, 
        pool_size: int=10**3,
        pages: int=1
    ) -> None:
        self.pool_size = pool_size
        self.pages = pages
        self.config = {}
        self.context: Optional[BrowserContext] = None
        self.sessions: Deque[BrowserSession] = deque()
        self.sem = asyncio.Semaphore(self.pool_size)
        self.timeouts = Timeouts()
        self.results: List[PlaywrightResult] = []
        self._active: Deque[Tuple[
            BrowserSession,
            Dict[
                Literal[
                    'command_start',
                    'command_end'
                ],
                float
            ],
            Page
        ]] = deque()

    async def start(
        self,
        browser_type: Literal[
            'safari',
            'webkit',
            'firefox',
            'chrome'
        ]=None,
        device_type: str=None, 
        locale: str=None, 
        geolocation: Geolocation=None, 
        permissions: List[str]=None, 
        color_scheme: str=None, 
        options: Dict[str, Any]={}
    ):
    
        playwright = await async_playwright().start()

        self.sessions.extend([
            BrowserSession(playwright, self.pages) for _ in self.concurrency
        ])

        await asyncio.gather(*[
            session.open(
                browser_type=browser_type,
                device_type=device_type,
                locale=locale,
                geolocation=geolocation,
                permissions=permissions,
                color_scheme=color_scheme,
                options=options,
                timeout=self.timeouts.request_timeout
            ) for session in self.sessions
        ])

    async def next_context(self):
        await self.sem.acquire()
        session = self.sessions.popleft()
        return session.context

    async def get_page(self):
        await self.sem.acquire()
        session = self.sessions.popleft()

        page = await session.next_page()

        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        timings['command_start'] = time.monotonic()

        self._active.append((
            session,
            timings,
            page
        ))
            
        return page
         
    async def __aenter__(self):
        await self.sem.acquire()

        session = self.sessions.popleft()
        page = await session.next_page()

        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        timings['command_start'] = time.monotonic()

        self._active.append((
            session,
            timings,
            page
        ))

        return page
    
    async def __aexit__(self):
        session, timings, page = self._active.popleft()

        timings['command_start'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        self.sem.release()


    async def goto(
        self, 
        url: str,
        wait_util: Optional[Literal[
            'commit', 
            'domcontentloaded', 
            'load', 
            'networkidle',
        ]]=None,
        referrer: Optional[str]=None,
        timeout: Optional[int | float]=None
    ):
        async with self.sem:

            timings: Dict[
                Literal[
                    'command_start',
                    'command_end'
                ],
                float
            ] = {}

            if timeout is None:
                timeout = self.timeouts.request_timeout

            session = self.sessions.popleft()

            command = GoToCommand(
                url=url,
                timeout=timeout,
                wait_util=wait_util,
                referrer=referrer
            )

            result: Optional[Response] = None
            err: Optional[Exception] = None

            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:
                result = await page.goto(
                    command.url,
                    timeout=command.timeout,
                    wait_until=command.wait_util,
                    referer=command.referrer
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)
                
                return PlaywrightResult(
                    command='goto',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='goto',
                command_args=command,
                metadata=session.metadata,
                result=result,
                timings=timings
            )

    async def get_url(
        self,
        timeout: Optional[int | float]=None,
    ):
        
        async with self.sem:

            timings: Dict[
                Literal[
                    'command_start',
                    'command_end'
                ],
                float
            ] = {}

            if timeout is None:
                timeout = self.timeouts.request_timeout

            session = self.sessions.popleft()

            command = GetUrlCommand(
                timeout=timeout
            )

            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            page_url = page.url  

            timings['command_end'] = time.monotonic() 
              
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='get_url',
                command_args=command,
                metadata=session.metadata,
                result=page_url,
                timings=timings
            )

    async def fill(
        self,
        selector: str,
        value: str,
        no_wait_after: Optional[bool]=None,
        strict: Optional[bool]=None,
        force: Optional[bool]=None,
        timeout: Optional[int | float]=None
    ):
        async with self.sem:

            timings: Dict[
                Literal[
                    'command_start',
                    'command_end'
                ],
                float
            ] = {}

            if timeout is None:
                timeout = self.timeouts.request_timeout

            session = self.sessions.popleft()

            command = FillCommand(
                selector=selector,
                value=value,
                no_wait_after=no_wait_after,
                strict=strict,
                force=force,
                timeout=timeout,
            )

            err: Optional[Exception] = None
            timings['command_start'] = time.monotonic()

            page = await session.next_page()

            try:

                await page.fill(
                    command.selector,
                    command.value,
                    no_wait_after=command.no_wait_after,
                    strict=command.strict,
                    force=command.force,
                    timeout=command.timeout,
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='fill',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='fill',
                command_args=command,
                result=None,
                metadata=session.metadata,
                timings=timings
            )

    async def check(
        self,
        selector: str,
        postion: Optional[Position]=None,
        timeout: Optional[int | float]=None,
        force: Optional[bool]=None,
        no_wait_after: Optional[bool]=None,
        strict: Optional[bool]=None,
        trial: Optional[bool]=None
    ):
        
        async with self.sem:

            timings: Dict[
                Literal[
                    'command_start',
                    'command_end'
                ],
                float
            ] = {}

            if timeout is None:
                timeout = self.timeouts.request_timeout

            session = self.sessions.popleft()

            command = CheckCommand(
                selector=selector,
                postion=postion,
                no_wait_after=no_wait_after,
                strict=strict,
                force=force,
                trial=trial,
                timeout=timeout,
            )

            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                await page.check(
                    command.selector,
                    position=command.postion,
                    force=command.force,
                    no_wait_after=command.no_wait_after,
                    strict=command.strict,
                    trial=command.trial,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='check',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='check',
                command_args=command,
                metadata=session.metadata,
                result=None,
                timings=timings
            )
    
    async def click(
        self,
        selector: str,
        modifiers: Optional[Sequence[Literal['Alt', 'Control', 'ControlOrMeta', 'Meta', 'Shift']]]=None,
        delay: Optional[int | float]=None,
        button: Optional[Literal['left', 'middle', 'right']]=None,
        click_count: Optional[int]=None,
        postion: Optional[Position]=None,
        timeout: Optional[int | float]=None,
        force: Optional[bool]=None,
        no_wait_after: Optional[bool]=None,
        strict: Optional[bool]=None,
        trial: Optional[bool]=None
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = ClickCommand(
                selector=selector,
                modifiers=modifiers,
                delay=delay,
                button=button,
                click_count=click_count,
                postion=postion,
                no_wait_after=no_wait_after,
                strict=strict,
                force=force,
                trial=trial,
                timeout=timeout,
            )

            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:
            
                await page.click(
                    command.selector,
                    position=command.postion,
                    modifiers=modifiers,
                    delay=delay,
                    button=button,
                    click_count=click_count,
                    force=command.force,
                    no_wait_after=command.no_wait_after,
                    strict=command.strict,
                    trial=command.trial,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='click',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='click',
                command_args=command,
                metadata=session.metadata,
                result=None,
                timings=timings
            )

    async def double_click(
        self,
        selector: str,
        modifiers: Optional[Sequence[Literal['Alt', 'Control', 'ControlOrMeta', 'Meta', 'Shift']]]=None,
        delay: Optional[int | float]=None,
        button: Optional[Literal['left', 'middle', 'right']]=None,
        postion: Optional[Position]=None,
        timeout: Optional[int | float]=None,
        force: Optional[bool]=None,
        no_wait_after: Optional[bool]=None,
        strict: Optional[bool]=None,
        trial: Optional[bool]=None
    ):
        
        async with self.sem:

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

            session = self.sessions.popleft()

            command = DoubleClickCommand(
                selector=selector,
                modifiers=modifiers,
                delay=delay,
                button=button,
                postion=postion,
                no_wait_after=no_wait_after,
                strict=strict,
                force=force,
                trial=trial,
                timeout=timeout,
            )

            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                await page.dblclick(
                    command.selector,
                    position=command.postion,
                    modifiers=modifiers,
                    delay=delay,
                    button=button,
                    force=command.force,
                    no_wait_after=command.no_wait_after,
                    strict=command.strict,
                    trial=command.trial,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='double_click',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='double_click',
                command_args=command,
                metadata=session.metadata,
                result=None,
                timings=timings
            )

    async def dispatch_event(
        self,
        selector: str,
        event_type: str,
        event_init: Optional[Dict[str, Any]]=None,
        strict: Optional[bool]=None,
        timeout: int | float=None
    ):
        
        async with self.sem:

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

            session = self.sessions.popleft()

            command = DispatchEventCommand(
                selector=selector,
                event_type=event_type,
                event_init=event_init,
                strict=strict,
                timeout=timeout
            )

            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                await page.dispatch_event(
                    command.selector,
                    type=command.event_type,
                    event_init=command.event_init,
                    strict=command.strict,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='dispatch_event',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='dispatch_event',
                command_args=command,
                metadata=session.metadata,
                result=None,
                timings=timings
            )

    async def drag_and_drop(
        self,
        source: str,
        target: str,
        source_position: Optional[Position]=None,
        target_position: Optional[Position]=None,
        timeout: Optional[int | float]=None,
        force: Optional[bool]=None,
        no_wait_after: Optional[bool]=None,
        strict: Optional[bool]=None,
        trial: Optional[bool]=None
    ):
        
        async with self.sem:

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

            session = self.sessions.popleft()

            command = DragAndDropCommand(
                source=source,
                target=target,
                source_position=source_position,
                target_position=target_position,
                force=force,
                no_wait_after=no_wait_after,
                strict=strict,
                trial=trial,
                timeout=timeout,
            )

            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                await page.drag_and_drop(
                    command.source,
                    command.target,
                    source_position=command.source_position,
                    target_position=command.target_position,
                    force=command.force,
                    no_wait_after=command.no_wait_after,
                    strict=command.strict,
                    trial=command.trial,
                    timeout=command.timeout,
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='drag_and_drop',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='drag_and_drop',
                command_args=command,
                metadata=session.metadata,
                result=None,
                timings=timings
            )

    async def bring_to_front(
        self,
        timeout: Optional[int | float]=None
    ):
        
        async with self.sem:

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

            session = self.sessions.popleft()

            command = BringToFrontCommand(
                timeout=timeout
            )

            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                await asyncio.wait_for(
                    page.bring_to_front(),
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='bring_to_front',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='bring_to_front',
                command_args=command,
                metadata=session.metadata,
                result=None,
                timings=timings
            )

    async def evaluate_on_selector(
        self,
        selector: str,
        expression: str,
        arg: Any,
        strict: Optional[bool]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = EvaluateOnSelectorCommand(
                selector=selector,
                expression=expression,
                arg=arg,
                strict=strict,
                timeout=timeout
            )

            result: Any = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await asyncio.wait_for(
                    page.eval_on_selector(
                        command.selector,
                        command.expression,
                        arg=command.arg,
                        strict=command.strict,
                    ),
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='eval_on_selector',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='eval_on_selector',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def evaluate_on_selectors_all(
        self,
        selector: str,
        expression: str,
        arg: Any,
        timeout: Optional[int | float]=None,
    ):
        
        async with self.sem:

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

            session = self.sessions.popleft()

            command = EvaluateOnSelectorCommand(
                selector=selector,
                expression=expression,
                arg=arg,
                timeout=timeout
            )

            result: Any = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await asyncio.wait_for(
                    page.eval_on_selector_all(
                        command.selector,
                        command.expression,
                        arg=command.arg,
                        strict=command.strict,
                    ),
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='eval_on_selector_all',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='eval_on_selector_all',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def evaluate(
        self,
        expression: str,
        arg: Any,
        timeout: Optional[int | float]=None,
    ):
        
        async with self.sem:

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

            session = self.sessions.popleft()

            command = EvaluateCommand(
                expression=expression,
                arg=arg,
                timeout=timeout
            )

            result: Any = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await asyncio.wait_for(
                    page.evaluate(
                        command.expression,
                        arg=command.arg,
                    ),
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='evaluate',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='evaluate',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def evaluate_handle(
        self,
        expression: str,
        arg: Any,
        timeout: Optional[int | float]=None,
    ):
        
        async with self.sem:

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

            session = self.sessions.popleft()

            command = EvaluateCommand(
                expression=expression,
                arg=arg,
                timeout=timeout
            )

            result: JSHandle = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await asyncio.wait_for(
                    page.evaluate_handle(
                        command.expression,
                        arg=command.arg,
                    ),
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='evaluate_handle',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='evaluate_handle',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def expect_console_message(
        self,
        predicate: Optional[
            Callable[
                [ConsoleMessage],
                bool | Awaitable[bool]
            ]
        ]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = ExpectConsoleMessageCommand(
                predicate=predicate,
                timeout=timeout
            )

            result: ConsoleMessage = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                if command.predicate is None:
                    message_response: AsyncEventContextManager[ConsoleMessage] = await page.expect_console_message(
                        timeout=command.timeout
                    )

                    async with message_response as message:
                        result = message

                else:
                    message_response: AsyncEventContextManager[ConsoleMessage] = await page.expect_console_message(
                        predicate=command.predicate,
                        timeout=command.timeout
                    )

                    async with message_response as message:
                        result = message

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='expect_console_message',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='expect_console_message',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )
            

    async def expect_download(
        self,
        predicate: Optional[
            Callable[
                [Download],
                bool
            ]
        ]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = ExpectDownloadCommand(
                predicate=predicate,
                timeout=timeout
            )

            result: Download = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                if command.predicate is None:
                    download_response: AsyncEventContextManager[Download] = await page.expect_download(
                        timeout=command.timeout
                    )

                    async with download_response as download:
                        result = download

                else:
                    download_response: AsyncEventContextManager[Download] = await page.expect_download(
                        predicate=command.predicate,
                        timeout=command.timeout
                    )

                    async with download_response as download:
                        result = download

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='expect_download',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='expect_download',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )


    async def expect_event(
        self,
        event: str,
        predicate: Optional[
            Callable[
                [ConsoleMessage],
                bool
            ]
        ]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = ExpectEventCommand(
                event=event,
                predicate=predicate,
                timeout=timeout
            )

            result: Any = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                if command.predicate is None:
                    event_response: AsyncEventContextManager[Any] = await page.expect_event(
                        command.event,
                        timeout=command.timeout
                    )

                    async with event_response as event:
                        result = event

                else:
                    event_response: AsyncEventContextManager[Any] = await page.expect_event(
                        command.event,
                        predicate=command.predicate,
                        timeout=command.timeout
                    )

                    async with event_response as event:
                        result = event

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='expect_event',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='expect_event',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def expect_file_chooser(
        self,
        predicate: Optional[
            Callable[
                [FileChooser],
                bool
            ]
        ]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = ExpectFileChooserCommand(
                predicate=predicate,
                timeout=timeout
            )

            result: Any = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                if command.predicate is None:
                    file_chooser_response: AsyncEventContextManager[Any] = await page.expect_file_chooser(
                        timeout=command.timeout
                    )

                    async with file_chooser_response as file_chooser:
                        result = file_chooser

                else:
                    file_chooser_response: AsyncEventContextManager[Any] = await  page.expect_file_chooser(
                        predicate=command.predicate,
                        timeout=command.timeout
                    )

                    async with file_chooser_response as file_chooser:
                        result = file_chooser

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='expect_file_chooser',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='expect_file_chooser',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def expect_navigation(
        self,
        url: str,
        wait_until: Optional[
            Literal['commit', 'domcontentloaded', 'load', 'networkidle']
        ]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = ExpectNavigationCommand(
                url=url,
                wait_until=wait_until,
                timeout=timeout
            )

            result: Response = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                navigation_response: AsyncEventContextManager[Response] = await page.expect_navigation(
                    command.url,
                    wait_until=command.wait_until,
                    timeout=command.timeout
                )

                async with navigation_response as navigation:
                    result = navigation

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='expect_navigation',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='expect_navigation',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def expect_popup(
        self,
        predicate: Optional[
            Callable[
                [Page],
                bool
            ]
        ]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = ExpectPopupCommand(
                predicate=predicate,
                timeout=timeout
            )

            result: Page = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                if command.predicate is None:
                    popup_response: AsyncEventContextManager[Page] = await page.expect_popup(
                        timeout=command.timeout
                    )

                    async with popup_response as popup:
                        result = popup

                else:
                    popup_response: AsyncEventContextManager[Page] = await page.expect_popup(
                        predicate=command.predicate,
                        timeout=command.timeout
                    )

                    async with popup_response as popup:
                        result = popup
            
            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='expect_popup',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='expect_popup',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def expect_request(
        self,
        url_or_predicate: Optional[
            str |
            Pattern[str] |
            Callable[
                [Page],
                bool
            ]
        ]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = ExpectRequestCommand(
                url_or_predicate=url_or_predicate,
                timeout=timeout
            )

            result: Request = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                if command.url_or_predicate is None:
                    response: AsyncEventContextManager[Request] = await page.expect_request(
                        timeout=command.timeout
                    )

                    async with response as resp:
                        result = resp

                else:
                    response: AsyncEventContextManager[Request] = await page.expect_request(
                        url_or_predicate=command.url_or_predicate,
                        timeout=command.timeout
                    )

                    async with response as resp:
                        result = resp

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='expect_request',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='expect_request',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def expect_request_finished(
        self,
        predicate: Optional[
            Callable[
                [Request],
                bool
            ]
        ]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = ExpectRequestFinishedCommand(
                predicate=predicate,
                timeout=timeout
            )

            result: Request = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                if command.predicate is None:
                    request: AsyncEventContextManager[Request] = await page.expect_request_finished(
                        timeout=command.timeout
                    )

                    async with request as req:
                        result = req

                else:
                    request: AsyncEventContextManager[Request] = await page.expect_request_finished(
                        predicate=command.predicate,
                        timeout=command.timeout
                    )

                    async with request as req:
                        result = req

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='expect_request_finished',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='expect_request_finished',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )
            
    async def expect_response(
        self,
        url_or_predicate: Optional[
            str | 
            Pattern[str] | 
            Callable[
                [Response],
                bool
            ]
        ]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = ExpectResponseCommand(
                url_or_predicate=url_or_predicate,
                timeout=timeout
            )

            result: Response = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                if command.url_or_predicate is None:
                    response: AsyncEventContextManager[Response] = await page.expect_response(
                        timeout=command.timeout
                    )

                    async with response as resp:
                        result = resp

                else:
                    response: AsyncEventContextManager[Response] = await page.expect_response(
                        url_or_predicate=command.url_or_predicate,
                        timeout=command.timeout
                    )

                    async with response as resp:
                        result = resp
            
            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='expect_response',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='expect_response',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )
    
    async def focus(
        self,
        selector: str,
        strict: Optional[bool]=False,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = FocusCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                await page.focus(
                    command.selector,
                    strict=command.strict,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='focus',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='focus',
                command_args=command,
                metadata=session.metadata,
                result=None,
                timings=timings
            )

    async def hover(
        self,
        selector: str,
        modifiers: Optional[Sequence[Literal['Alt', 'Control', 'ControlOrMeta', 'Meta', 'Shift']]]=None,
        postion: Optional[Position]=None,
        force: Optional[bool]=None,
        no_wait_after: Optional[bool]=None,
        strict: Optional[bool]=None,
        trial: Optional[bool]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = HoverCommand(
                selector=selector,
                modifiers=modifiers,
                postion=postion,
                force=force,
                no_wait_after=no_wait_after,
                trial=trial,
                strict=strict,
                timeout=timeout
            )

            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                await page.hover(
                    command.selector,
                    modifiers=command.modifiers,
                    position=command.postion,
                    force=command.force,
                    no_wait_after=command.no_wait_after,
                    trial=command.trial,
                    strict=command.strict,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='hover',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='hover',
                command_args=command,
                metadata=session.metadata,
                result=None,
                timings=timings
            )
    
    async def inner_html(
        self,
        selector: str,
        strict: Optional[bool]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            result: str = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await page.inner_html(
                    command.selector,
                    strict=command.strict,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='inner_html',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='inner_html',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def inner_text(
        self,
        selector: str,
        strict: Optional[bool]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            result: str = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await page.inner_text(
                    command.selector,
                    strict=command.strict,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='inner_text',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='inner_text',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def input_value(
        self,
        selector: str,
        strict: Optional[bool]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            result: str = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await page.input_value(
                    command.selector,
                    strict=command.strict,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='input_value',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='input_value',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def press(
        self,
        selector: str,
        key: str,
        delay: Optional[int | float] = None,
        no_wait_after: Optional[bool] = None,
        strict: Optional[bool] = None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = PressCommand(
                selector=selector,
                key=key,
                delay=delay,
                no_wait_after=no_wait_after,
                strict=strict,
                timeout=timeout
            )

            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                await page.press(
                    command.selector,
                    strict=command.strict,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='press',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='press',
                command_args=command,
                metadata=session.metadata,
                result=None,
                timings=timings
            )

    async def is_enabled(
        self,
        selector: str,
        strict: Optional[bool] = None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            result: bool = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await page.is_enabled(
                    command.selector,
                    strict=command.strict,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='is_enabled',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='is_enabled',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def is_hidden(
        self,
        selector: str,
        strict: Optional[bool] = None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            result: bool = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await page.is_hidden(
                    command.selector,
                    strict=command.strict,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='is_hidden',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='is_hidden',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def is_visible(
        self,
        selector: str,
        strict: Optional[bool] = None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            result: bool = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await page.is_visible(
                    command.selector,
                    strict=command.strict,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='is_visible',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='is_visible',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def is_checked(
        self,
        selector: str,
        strict: Optional[bool] = None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

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

            session = self.sessions.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            result: bool = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await page.is_checked(
                    command.selector,
                    strict=command.strict,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='is_checked',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='is_checked',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def content(
        self,
        timeout: Optional[int | float]=None,
    ) -> PlaywrightResult[Exception] | PlaywrightResult[str]:
        async with self.sem:

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

            session = self.sessions.popleft()

            command = ContentCommand(
                timeout=timeout
            )

            result: str = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await page.content(
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='content',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='content',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            )

    async def query_selector(
        self,
        selector: str,
        strict: Optional[bool]=None,
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

        session = self.sessions.popleft()

        command = DOMCommand(
            selector=selector,
            strict=strict,
            timeout=timeout
        )
    
        result: Optional[ElementHandle] = None
        err: Optional[Exception] = None
        page = await session.next_page()

        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                page.query_selector(
                    command.selector,
                    strict=command.strict
                ),
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()
            
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='query_selector',
                command_args=command,
                metadata=session.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        return PlaywrightResult(
            command='query_selector',
            command_args=command,
            result=result,
            metadata=session.metadata,
            timings=timings
        )

    async def get_all_elements(
        self,
        selector: str,
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

        session = self.sessions.popleft()
    
        command = DOMCommand(
            selector=selector,
            timeout=timeout
        )
    
        result: List[ElementHandle] = []
        err: Optional[Exception] = None
        page = await session.next_page()

        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                page.query_selector_all(command.selector),
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()
            
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='query_selector_all',
                command_args=command,
                metadata=session.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        return PlaywrightResult(
            command='query_selector_all',
            command_args=command,
            result=result,
            metadata=session.metadata,
            timings=timings
        )

    async def reload_page(
        self,
        wait_until: Optional[
            Literal[
                'commit', 
                'domcontentloaded', 
                'load', 
                'networkidle'
            ]
        ] = None,
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

        session = self.sessions.popleft()

        command = ReloadCommand(
            wait_util=wait_until,
            timeout=timeout
        )
      
        result: Optional[Response] = None
        err: Optional[Exception] = None
        page = await session.next_page()

        timings['command_start'] = time.monotonic()

        try:

            result = await page.reload(
                wait_until=command.wait_util,
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()
            
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='reload',
                command_args=command,
                metadata=session.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        return PlaywrightResult(
            command='reload',
            command_args=command,
            result=result,
            metadata=session.metadata,
            timings=timings
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

        session = self.sessions.popleft()

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
        page = await session.next_page()

        timings['command_start'] = time.monotonic()

        try:

            result = await page.screenshot(
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
            
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='screenshot',
                command_args=command,
                metadata=session.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        return PlaywrightResult(
            command='screenshot',
            command_args=command,
            result=result,
            metadata=session.metadata,
            timings=timings
        )

    async def select_option(
        self,
        selector: str,
        value: Optional[str | Sequence[str]] = None,
        index: Optional[int | Sequence[int]] = None,
        label: Optional[str | Sequence[str]] = None,
        element: Optional[ElementHandle | Sequence[ElementHandle]] = None,
        no_wait_after: Optional[bool] = None,
        force: Optional[bool] = None,
        strict: Optional[bool] = None,
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

        session = self.sessions.popleft()
    
        command = SelectOptionCommand(
            selector=selector,
            value=value,
            index=index,
            label=label,
            element=element,
            no_wait_after=no_wait_after,
            force=force,
            strict=strict,
            timeout=timeout
        )
    
        result: List[str] = []
        err: Optional[Exception] = None
        page = await session.next_page()

        timings['command_start'] = time.monotonic()

        try:

            await page.select_option(
                command.selector,
                value=command.validate,
                index=command.index,
                label=command.label,
                element=command.element,
                no_wait_after=command.no_wait_after,
                force=command.force,
                strict=command.strict,
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()
            
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='select_option',
                command_args=command,
                metadata=session.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        return PlaywrightResult(
            command='select_option',
            command_args=command,
            result=result,
            metadata=session.metadata,
            timings=timings
        )

    async def set_checked(
        self,
        selector: str,
        checked: bool,
        position: Optional[Position] = None,
        force: Optional[bool] = None,
        no_wait_after: Optional[bool] = None,
        strict: Optional[bool] = None,
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

        session = self.sessions.popleft()
    
        command = SetCheckedCommand(
            selector=selector,
            checked=checked,
            position=position,
            force=force,
            no_wait_after=no_wait_after,
            strict=strict,
            trial=trial,
            timeout=timeout
        )
    
        err: Optional[Exception] = None
        page = await session.next_page()

        timings['command_start'] = time.monotonic()

        try:

            await page.set_checked(
                command.selector,
                command.checked,
                position=command.position,
                force=command.force,
                no_wait_after=command.no_wait_after,
                strict=command.strict,
                trial=command.trial,
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()
            
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='set_checked',
                command_args=command,
                metadata=session.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        return PlaywrightResult(
            command='set_checked',
            command_args=command,
            result=None,
            metadata=session.metadata,
            timings=timings
        )
    
    async def set_default_timeout(
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

        session = self.sessions.popleft()
    
        command = SetTimeoutCommand(
            timeout=timeout
        )
    
        err: Optional[Exception] = None
        page = await session.next_page()

        timings['command_start'] = time.monotonic()

        try:

            await page.set_default_timeout(
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()
            
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='set_default_timeout',
                command_args=command,
                metadata=session.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        return PlaywrightResult(
            command='set_default_timeout',
            command_args=command,
            result=None,
            metadata=session.metadata,
            timings=timings
        )
    
    async def set_default_navigation_timeout(
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

        session = self.sessions.popleft()
    
        command = SetTimeoutCommand(
            timeout=timeout
        )
    
        err: Optional[Exception] = None
        page = await session.next_page()

        timings['command_start'] = time.monotonic()

        try:

            await page.set_default_navigation_timeout(
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()
            
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='set_default_navigation_timeout',
                command_args=command,
                metadata=session.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        return PlaywrightResult(
            command='set_default_navigation_timeout',
            command_args=command,
            result=None,
            metadata=session.metadata,
            timings=timings
        )
    
    async def set_extra_http_headers(
        self,
        headers: Dict[str, str],
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

        session = self.sessions.popleft()
    
        command = SetExtraHTTPHeadersCommand(
            headers=headers,
            timeout=timeout
        )
    
        err: Optional[Exception] = None
        page = await session.next_page()

        timings['command_start'] = time.monotonic()

        try:

            await asyncio.wait_for(
                page.set_extra_http_headers(
                    command.headers
                ),
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()
            
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='set_extra_http_headers',
                command_args=command,
                metadata=session.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        return PlaywrightResult(
            command='set_extra_http_headers',
            command_args=command,
            result=None,
            metadata=session.metadata,
            timings=timings
        )

    async def tap(
        self,
        selector: str,
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
        strict: Optional[bool] = None,
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

        session = self.sessions.popleft()
    
        command = TapCommand(
            selector=selector,
            modifiers=modifiers,
            position=position,
            force=force,
            no_wait_after=no_wait_after,
            strict=strict,
            trial=trial,
            timeout=timeout
        )
    
        err: Optional[Exception] = None
        page = await session.next_page()

        timings['command_start'] = time.monotonic()

        try:

            await page.tap(
                command.selector,
                modifiers=command.modifiers,
                position=command.position,
                force=command.force,
                no_wait_after=command.no_wait_after,
                strict=command.strict,
                trial=command.trial,
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()
            
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='tap',
                command_args=command,
                metadata=session.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        return PlaywrightResult(
            command='tap',
            command_args=command,
            result=None,
            metadata=session.metadata,
            timings=timings
        )
    
    async def text_content(
        self,
        selector: str,
        strict: Optional[bool] = None,
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

        session = self.sessions.popleft()
    
        command = DOMCommand(
            selector=selector,
            strict=strict,
            timeout=timeout
        )

        result: Optional[str]=None
        err: Optional[Exception] = None
        page = await session.next_page()

        timings['command_start'] = time.monotonic()

        try:

            result = await page.text_content(
                command.selector,
                strict=command.strict,
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()
            
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='text_content',
                command_args=command,
                metadata=session.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        return PlaywrightResult(
            command='text_content',
            command_args=command,
            result=result,
            metadata=session.metadata,
            timings=timings
        )
    
    async def title(
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

        session = self.sessions.popleft()
    
        command = TitleCommand(
            timeout=timeout
        )

        result: str=None
        err: Optional[Exception] = None
        page = await session.next_page()

        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                page.title(),
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()
            
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='title',
                command_args=command,
                metadata=session.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        return PlaywrightResult(
            command='title',
            command_args=command,
            result=result,
            metadata=session.metadata,
            timings=timings
        )
    
    async def type_text(
        self,
        selector: str,
        text: str,
        delay: Optional[int | float] = None,
        no_wait_after: Optional[bool] = None,
        strict: Optional[bool] = None,
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

        session = self.sessions.popleft()
    
        command = TypeCommand(
            selector=selector,
            text=text,
            delay=delay,
            no_wait_after=no_wait_after,
            strict=strict,
            timeout=timeout
        )

        err: Optional[Exception] = None
        page = await session.next_page()

        timings['command_start'] = time.monotonic()

        try:

            await page.type(
                    command.selector,
                    command.text,
                    delay=command.delay,
                    no_wait_after=command.no_wait_after,
                    strict=command.strict,
                    timeout=command.timeout
                )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()
            
            session.return_page(page)
            self.sessions.append(session)

            return PlaywrightResult(
                command='type_text',
                command_args=command,
                metadata=session.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        session.return_page(page)
        self.sessions.append(session)

        return PlaywrightResult(
            command='type_text',
            command_args=command,
            result=None,
            metadata=session.metadata,
            timings=timings
        )
    
    async def uncheck(
        self,
        selector: str,
        postion: Optional[Position]=None,
        timeout: Optional[int | float]=None,
        force: Optional[bool]=None,
        no_wait_after: Optional[bool]=None,
        strict: Optional[bool]=None,
        trial: Optional[bool]=None
    ):
        
        async with self.sem:

            timings: Dict[
                Literal[
                    'command_start',
                    'command_end'
                ],
                float
            ] = {}

            if timeout is None:
                timeout = self.timeouts.request_timeout

            session = self.sessions.popleft()

            command = CheckCommand(
                selector=selector,
                postion=postion,
                no_wait_after=no_wait_after,
                strict=strict,
                force=force,
                trial=trial,
                timeout=timeout,
            )

            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                await page.uncheck(
                    command.selector,
                    position=command.postion,
                    force=command.force,
                    no_wait_after=command.no_wait_after,
                    strict=command.strict,
                    trial=command.trial,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='uncheck',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='uncheck',
                command_args=command,
                metadata=session.metadata,
                result=None,
                timings=timings
            )

    async def wait_for_event(
        self,
        event: str,
        predicate: Optional[
            Callable[
                [ConsoleMessage],
                bool
            ]
        ]=None,
        timeout: Optional[int | float]=None,
    ):
        
        async with self.sem:

            timings: Dict[
                Literal[
                    'command_start',
                    'command_end'
                ],
                float
            ] = {}

            if timeout is None:
                timeout = self.timeouts.request_timeout

            session = self.sessions.popleft()

            command = ExpectEventCommand(
                event=event,
                predicate=predicate,
                timeout=timeout,
            )

            result: Any = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await page.wait_for_event(
                    event=command.event,
                    predicate=command.predicate,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='wait_for_event',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='wait_for_event',
                command_args=command,
                metadata=session.metadata,
                result=result,
                timings=timings
            )
        
    async def wait_for_function(
        self,
        expression: str,
        arg: Optional[Any] = None,
        polling: Optional[float | Literal['raf']] = None,
        timeout: Optional[int | float]=None,
    ):
        
        async with self.sem:

            timings: Dict[
                Literal[
                    'command_start',
                    'command_end'
                ],
                float
            ] = {}

            if timeout is None:
                timeout = self.timeouts.request_timeout

            session = self.sessions.popleft()

            command = WaitForFunctionCommand(
                expression=expression,
                arg=arg,
                polling=polling,
                timeout=timeout
            )

            result: JSHandle = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await page.wait_for_function(
                    command.expression,
                    arg=command.arg,
                    polling=command.polling,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='wait_for_function',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='wait_for_function',
                command_args=command,
                metadata=session.metadata,
                result=result,
                timings=timings
            )
        
    async def wait_for_load_state(
        self,
        state: Optional[
            Literal[
                'domcontentloaded', 
                'load', 
                'networkidle'
            ]
        ]=None,
        timeout: Optional[int | float]=None,
    ):
        
        async with self.sem:

            timings: Dict[
                Literal[
                    'command_start',
                    'command_end'
                ],
                float
            ] = {}

            if timeout is None:
                timeout = self.timeouts.request_timeout

            session = self.sessions.popleft()

            command = WaitForLoadStateCommand(
                state=state,
                timeout=timeout
            )

            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                await page.wait_for_load_state(
                    state=command.state,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='wait_for_load_state',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='wait_for_load_state',
                command_args=command,
                metadata=session.metadata,
                result=None,
                timings=timings
            )
        
    async def wait_for_selector(
        self,
        selector: str,
        state: Optional[Literal['attached', 'detached', 'hidden', 'visible']] = None,
        strict: Optional[bool] = None,
        timeout: Optional[int | float]=None,
    ):
        
        async with self.sem:

            timings: Dict[
                Literal[
                    'command_start',
                    'command_end'
                ],
                float
            ] = {}

            if timeout is None:
                timeout = self.timeouts.request_timeout

            session = self.sessions.popleft()

            command = WaitForSelectorCommand(
                selector=selector,
                state=state,
                strict=strict,
                timeout=timeout
            )

            result: Optional[ElementHandle] = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = await page.wait_for_selector(
                    command.selector,
                    state=command.state,
                    strict=command.strict,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='wait_for_selector',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='wait_for_selector',
                command_args=command,
                metadata=session.metadata,
                result=result,
                timings=timings
            )
        
    async def wait_for_timeout(
        self,
        timeout: Optional[int | float]=None,
    ):
        
        async with self.sem:

            timings: Dict[
                Literal[
                    'command_start',
                    'command_end'
                ],
                float
            ] = {}

            if timeout is None:
                timeout = self.timeouts.request_timeout

            session = self.sessions.popleft()

            command = WaitForTimeoutCommand(
                timeout=timeout
            )

            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                await page.wait_for_timeout(
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='wait_for_timeout',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='wait_for_timeout',
                command_args=command,
                metadata=session.metadata,
                result=None,
                timings=timings
            )
        
    async def wait_for_url(
        self,
        url: str | Pattern[str] | Callable[[str], bool],
        wait_until: Optional[
            Literal[
                'commit', 
                'domcontentloaded', 
                'load', 
                'networkidle'
            ]
        ] = None,
        timeout: Optional[int | float]=None,
    ):
        
        async with self.sem:

            timings: Dict[
                Literal[
                    'command_start',
                    'command_end'
                ],
                float
            ] = {}

            if timeout is None:
                timeout = self.timeouts.request_timeout

            session = self.sessions.popleft()

            command = WaitForUrlCommand(
                url=url,
                wait_until=wait_until,
                timeout=timeout
            )

            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                await page.wait_for_url(
                    command.url,
                    wait_until=command.wait_until,
                    timeout=command.timeout
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='wait_for_url',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='wait_for_url',
                command_args=command,
                metadata=session.metadata,
                result=None,
                timings=timings
            )

    async def frame(
        self,
        name: Optional[str] = None,
        url: Optional[str | Pattern[str] | Callable[[str], bool]]=None,
    ):
        
        async with self.sem:

            timings: Dict[
                Literal[
                    'command_start',
                    'command_end'
                ],
                float
            ] = {}

            session = self.sessions.popleft()

            command = FrameCommand(
                name=name,
                url=url
            )

            result: Optional[Frame] = None
            err: Optional[Exception] = None
            page = await session.next_page()

            timings['command_start'] = time.monotonic()

            try:

                result = page.frame(
                    name=command.name,
                    url=command.url
                )

            except Exception as err:
                
                timings['command_end'] = time.monotonic()
                
                session.return_page(page)
                self.sessions.append(session)

                return PlaywrightResult(
                    command='wait_for_url',
                    command_args=command,
                    metadata=session.metadata,
                    result=err,
                    error=str(err),
                    timings=timings
                )

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            return PlaywrightResult(
                command='wait_for_url',
                command_args=command,
                metadata=session.metadata,
                result=result,
                timings=timings
            )

    async def get_frames(self, command: PlaywrightCommand):
        return await self.page.frames()

    async def get_attribute(self, command: PlaywrightCommand):
        return await self.page.get_attribute(
            command.page.selector,
            command.page.attribute
        )

    async def go_back_page(self, command: PlaywrightCommand):
        await self.page.go_back(**command.options.extra)

    async def go_forward_page(self, command: PlaywrightCommand):
        await self.page.go_forward(**command.options.extra)



async def test():
    sess = MercurySyncPlaywrightClient()

    async with sess as page:
        pass