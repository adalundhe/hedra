
import asyncio
import time
from collections import deque
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
)

from playwright._impl._async_base import AsyncEventContextManager
from playwright.async_api import (
    BrowserContext,
    ConsoleMessage,
    Download,
    FileChooser,
    Geolocation,
    JSHandle,
    Page,
    Position,
    Request,
    Response,
    async_playwright,
)

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .browser_session import BrowserSession
from .command import PlaywrightCommand
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
    GetUrlCommand,
    GoToCommand,
    HoverCommand,
    PressCommand,
)
from .models.results import PlaywrightResult


class MercurySyncPlaywrightClient:


    def __init__(
        self, 
        concurrency: int,
        pages: int
    ) -> None:
        self.concurrency = concurrency
        self.pages = pages
        self.config = {}
        self.context: Optional[BrowserContext] = None
        self.sessions: Deque[BrowserSession] = deque()
        self.sem = asyncio.Semaphore(self.concurrency)
        self.timeouts = Timeouts()
        self.results: List[PlaywrightResult] = []

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
            BrowserSession(playwright) for _ in self.concurrency
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

    async def new_page(self):
         async with self.sem:
            session = self.sessions.popleft()

            page = await session.context.new_page()

            self.sessions.append(session)
             
            return page

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
                self.results.append(PlaywrightResult(
                    command='goto',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            self.results.append(PlaywrightResult(
                command='goto',
                command_args=command,
                metadata=session.metadata,
                result=result,
                timings=timings
            ))


            return result

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

            self.results.append(PlaywrightResult(
                command='get_url',
                command_args=command,
                metadata=session.metadata,
                result=page_url,
                timings=timings
            ))  

            return page_url

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

                self.results.append(PlaywrightResult(
                    command='fill',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            self.results.append(PlaywrightResult(
                command='fill',
                command_args=command,
                metadata=session.metadata,
                timings=timings
            ))

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

                self.results.append(PlaywrightResult(
                    command='check',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            self.results.append(PlaywrightResult(
                command='check',
                command_args=command,
                metadata=session.metadata,
                timings=timings
            ))
    
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

                self.results.append(PlaywrightResult(
                    command='click',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            self.results.append(PlaywrightResult(
                command='click',
                command_args=command,
                metadata=session.metadata,
                timings=timings
            ))

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

                self.results.append(PlaywrightResult(
                    command='double_click',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            self.results.append(PlaywrightResult(
                command='double_click',
                command_args=command,
                metadata=session.metadata,
                timings=timings
            ))


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

                self.results.append(PlaywrightResult(
                    command='dispatch_event',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            self.results.append(PlaywrightResult(
                command='dispatch_event',
                command_args=command,
                metadata=session.metadata,
                timings=timings
            ))

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

                self.results.append(PlaywrightResult(
                    command='drag_and_drop',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            self.results.append(PlaywrightResult(
                command='drag_and_drop',
                command_args=command,
                metadata=session.metadata,
                timings=timings
            ))

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

                self.results.append(PlaywrightResult(
                    command='bring_to_front',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            self.results.append(PlaywrightResult(
                command='bring_to_front',
                command_args=command,
                metadata=session.metadata,
                timings=timings
            ))

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

            result: Optional[Any] = None
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

                self.results.append(PlaywrightResult(
                    command='eval_on_selector',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            self.results.append(PlaywrightResult(
                command='eval_on_selector',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            ))

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

            result: Optional[Any] = None
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

                self.results.append(PlaywrightResult(
                    command='eval_on_selector_all',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            self.results.append(PlaywrightResult(
                command='eval_on_selector_all',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            ))

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

            result: Optional[Any] = None
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

                self.results.append(PlaywrightResult(
                    command='evaluate',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            self.results.append(PlaywrightResult(
                command='evaluate',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            ))

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

            result: Optional[JSHandle] = None
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

                self.results.append(PlaywrightResult(
                    command='evaluate_handle',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)
            
            self.results.append(PlaywrightResult(
                command='evaluate_handle',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            ))

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

            result: Optional[ConsoleMessage] = None
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

                self.results.append(PlaywrightResult(
                    command='expect_console_message',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            self.results.append(PlaywrightResult(
                command='expect_console_message',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            ))
            

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

            result: Optional[Download] = None
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

                self.results.append(PlaywrightResult(
                    command='expect_console_message',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            self.results.append(PlaywrightResult(
                command='expect_console_message',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            ))


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

            result: Optional[Download] = None
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

                self.results.append(PlaywrightResult(
                    command='expect_console_message',
                    command_args=command,
                    metadata=session.metadata,
                    error=str(err),
                    timings=timings
                ))

                return err

            timings['command_end'] = time.monotonic()

            session.return_page(page)
            self.sessions.append(session)

            self.results.append(PlaywrightResult(
                command='expect_console_message',
                command_args=command,
                result=result,
                metadata=session.metadata,
                timings=timings
            ))

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

            page = self.pages.popleft()

            command = ExpectFileChooserCommand(
                predicate=predicate,
                timeout=timeout
            )

            if command.predicate is None:
                await page.expect_file_chooser(
                    timeout=command.timeout
                )

            else:
                await page.expect_file_chooser(
                    predicate=command.predicate,
                    timeout=command.timeout
                )

            self.pages.append(page)

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

            page = self.pages.popleft()

            command = ExpectNavigationCommand(
                url=url,
                wait_until=wait_until,
                timeout=timeout
            )

            await page.expect_navigation(
                command.url,
                wait_until=command.wait_until,
                timeout=command.timeout
            )

            self.pages.append(page)

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

            page = self.pages.popleft()

            command = ExpectPopupCommand(
                predicate=predicate,
                timeout=timeout
            )

            if command.predicate is None:
                await page.expect_popup(
                    timeout=command.timeout
                )

            else:
                await page.expect_popup(
                    predicate=command.predicate,
                    timeout=command.timeout
                )


            self.pages.append(page)

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

            page = self.pages.popleft()

            command = ExpectRequestCommand(
                url_or_predicate=url_or_predicate,
                timeout=timeout
            )

            if command.url_or_predicate is None:
                await page.expect_request(
                    timeout=command.timeout
                )

            else:
                await page.expect_request(
                    url_or_predicate=command.url_or_predicate,
                    timeout=command.timeout
                )


            self.pages.append(page)

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

            page = self.pages.popleft()

            command = ExpectRequestFinishedCommand(
                predicate=predicate,
                timeout=timeout
            )

            if command.predicate is None:
                await page.expect_request_finished(
                    timeout=command.timeout
                )

            else:
                await page.expect_request_finished(
                    predicate=command.predicate,
                    timeout=command.timeout
                )

            self.pages.append(page)

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

            page = self.pages.popleft()

            command = ExpectResponseCommand(
                url_or_predicate=url_or_predicate,
                timeout=timeout
            )

            if command.url_or_predicate is None:
                await page.expect_response(
                    timeout=command.timeout
                )

            else:
                await page.expect_response(
                    url_or_predicate=command.url_or_predicate,
                    timeout=command.timeout
                )

            self.pages.append(page)
    
    async def focus(
        self,
        selector: str,
        strict: Optional[bool]=False,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

            if timeout is None:
                timeout = self.timeouts.request_timeout

            page = self.pages.popleft()

            command = FocusCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            await page.focus(
                command.selector,
                strict=command.strict,
                timeout=command.timeout
            )

            self.pages.append(page)

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

            page = self.pages.popleft()

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

            self.pages.append(page)
    
    async def inner_html(
        self,
        selector: str,
        strict: Optional[bool]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

            if timeout is None:
                timeout = self.timeouts.request_timeout

            page = self.pages.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            await page.inner_html(
                command.selector,
                strict=command.strict,
                timeout=command.timeout
            )

            self.pages.append(page)

    async def inner_text(
        self,
        selector: str,
        strict: Optional[bool]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

            if timeout is None:
                timeout = self.timeouts.request_timeout

            page = self.pages.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            await page.inner_text(
                command.selector,
                strict=command.strict,
                timeout=command.timeout
            )

            self.pages.append(page)

    async def input_value(
        self,
        selector: str,
        strict: Optional[bool]=None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

            if timeout is None:
                timeout = self.timeouts.request_timeout

            page = self.pages.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            await page.input_value(
                command.selector,
                strict=command.strict,
                timeout=command.timeout
            )

            self.pages.append(page)

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

            page = self.pages.popleft()

            command = PressCommand(
                selector=selector,
                key=key,
                delay=delay,
                no_wait_after=no_wait_after,
                strict=strict,
                timeout=timeout
            )

            await page.press(
                command.selector,
                strict=command.strict,
                timeout=command.timeout
            )

            self.pages.append(page)

    async def is_enabled(
        self,
        selector: str,
        strict: Optional[bool] = None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

            if timeout is None:
                timeout = self.timeouts.request_timeout

            page = self.pages.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            await page.is_enabled(
                command.selector,
                strict=command.strict,
                timeout=command.timeout
            )

            self.pages.append(page)

    async def is_hidden(
        self,
        selector: str,
        strict: Optional[bool] = None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

            if timeout is None:
                timeout = self.timeouts.request_timeout

            page = self.pages.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            await page.is_hidden(
                command.selector,
                strict=command.strict,
                timeout=command.timeout
            )

            self.pages.append(page)

    async def is_visible(
        self,
        selector: str,
        strict: Optional[bool] = None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

            if timeout is None:
                timeout = self.timeouts.request_timeout

            page = self.pages.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            await page.is_visible(
                command.selector,
                strict=command.strict,
                timeout=command.timeout
            )

            self.pages.append(page)

    async def is_checked(
        self,
        selector: str,
        strict: Optional[bool] = None,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

            if timeout is None:
                timeout = self.timeouts.request_timeout

            page = self.pages.popleft()

            command = DOMCommand(
                selector=selector,
                strict=strict,
                timeout=timeout
            )

            await page.is_checked(
                command.selector,
                strict=command.strict,
                timeout=command.timeout
            )

            self.pages.append(page)

    async def content(
        self,
        timeout: Optional[int | float]=None,
    ):
        async with self.sem:

            if timeout is None:
                timeout = self.timeouts.request_timeout

            page = self.pages.popleft()

            command = ContentCommand(
                timeout=timeout
            )

            await page.content(
                timeout=command.timeout
            )

            self.pages.append(page)

    async def get_element(self, command: PlaywrightCommand):
        return await self.page.query_selector(
            command.page.selector, 
            **command.options.extra
        )

    async def get_all_elements(self, command: PlaywrightCommand):
        return await self.page.query_selector_all(
            command.page.selector, 
            **command.options.extra
        )

    async def reload_page(self, command: PlaywrightCommand):
        await self.page.reload(**command.options.extra)

    async def take_screenshot(self, command: PlaywrightCommand):
        await self.page.screenshot(
            path=f'{command.input.path}.png', 
            **command.options.extra
        )

    async def select_option(self, command: PlaywrightCommand):

        if command.input.by_label:
            await self.page.select_option(
                command.page.selector, 
                label=command.input.option, 
                **command.options.extra
            )

        elif command.input.by_value:
            await self.page.select_option(
                command.page.selector, 
                value=command.input.option, 
                **command.options.extra
            )

        else:
            await self.page.select_option(
                command.page.selector, 
                command.input.option, 
                **command.options.extra
            )

    async def set_checked(self, command: PlaywrightCommand):
        await self.page.set_checked(
            command.page.selector, 
            command.options.is_checked, 
            **command.options.extra
        )

    async def set_default_timeout(self, command: PlaywrightCommand):
        await self.page.set_default_timeout(command.options.timeout)

    async def set_navigation_timeout(self, command: PlaywrightCommand):
        await self.page.set_default_navigation_timeout(command.options.timeout)

    async def set_http_headers(self, command: PlaywrightCommand):
        await self.page.set_extra_http_headers(command.url.headers)

    async def tap(self, command: PlaywrightCommand):
        await self.page.tap(
            command.page.selector, 
            **command.options.extra
        )

    async def get_text_content(self, command: PlaywrightCommand):
        await self.page.text_content(
            command.page.selector,
            **command.options.extra
        )

    async def get_page_title(self, command: PlaywrightCommand):
        await self.page.title()

    async def input_text(self, command: PlaywrightCommand):
        await self.page.type(
            command.page.selector,
            command.input.text, 
            **command.options.extra
        )

    async def uncheck(self, command: PlaywrightCommand):
        await self.page.uncheck(
            command.page.selector, 
            **command.options.extra
        )

    async def wait_for_event(self, command: PlaywrightCommand):
        await self.page.wait_for_event(
            command.options.event, 
            **command.options.extra
        )

    async def wait_for_function(self, command: PlaywrightCommand):
        await self.page.wait_for_function(
            command.input.expression, 
            *command.input.args, 
            **command.options.extra
        )

    async def wait_for_page_load_state(self, command: PlaywrightCommand):
        await self.page.wait_for_load_state(**command.options.extra)

    async def wait_for_selector(self, command: PlaywrightCommand):
        await self.page.wait_for_selector(
            command.page.selector, 
            **command.options.extra
        )

    async def wait_for_timeout(self, command: PlaywrightCommand):
        await self.page.wait_for_timeout(command.options.timeout)

    async def wait_for_url(self, command: PlaywrightCommand):
        await self.page.wait_for_url(
            command.url.location, 
            **command.options.extra
        )

    async def switch_frame(self, command: PlaywrightCommand):

        if command.url.location == 'url':
            await self.page.frame(url=command.url.location)

        else:
            await self.page.frame(name=command.page.frame)

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