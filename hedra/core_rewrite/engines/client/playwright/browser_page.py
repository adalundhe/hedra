import asyncio
import time
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Pattern,
    Sequence,
)

from playwright._impl._async_base import AsyncEventContextManager
from playwright.async_api import (
    ConsoleMessage,
    Dialog,
    Download,
    ElementHandle,
    Error,
    FileChooser,
    FilePayload,
    FloatRect,
    Frame,
    FrameLocator,
    JSHandle,
    Locator,
    Page,
    PdfMargins,
    Position,
    Request,
    Response,
    Route,
    ViewportSize,
    WebSocket,
    Worker,
)

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .browser_frame import BrowserFrame
from .browser_mouse import BrowserMouse
from .models.browser import BrowserMetadata
from .models.commands.page import (
    AddInitScriptCommand,
    AddLocatorHandlerCommand,
    AddScriptTagCommand,
    AddStyleTagCommand,
    BringToFrontCommand,
    CheckCommand,
    ClickCommand,
    CloseCommand,
    ContentCommand,
    DispatchEventCommand,
    DOMCommand,
    DoubleClickCommand,
    DragAndDropCommand,
    EmulateMediaCommand,
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
    ExpectWebsocketCommand,
    ExpectWorkerCommand,
    ExposeBindingCommand,
    ExposeFunctionCommand,
    FillCommand,
    FocusCommand,
    FrameCommand,
    FrameLocatorCommand,
    GetAttributeCommand,
    GetByRoleCommand,
    GetByTestIdCommand,
    GetByTextCommand,
    GetUrlCommand,
    GoCommand,
    GoToCommand,
    HoverCommand,
    IsClosedCommand,
    LocatorCommand,
    OnCommand,
    OpenerCommand,
    PauseCommand,
    PdfCommand,
    PressCommand,
    ReloadCommand,
    RemoveLocatorHandlerCommand,
    RouteCommand,
    RouteFromHarCommand,
    ScreenshotCommand,
    SelectOptionCommand,
    SetCheckedCommand,
    SetContentCommand,
    SetExtraHTTPHeadersCommand,
    SetInputFilesCommand,
    SetTimeoutCommand,
    SetViewportSize,
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


class BrowserPage:

    def __init__(
        self,
        page: Page,
        timeouts: Timeouts,
        metadata: BrowserMetadata
    ) -> None:
        self.page = page
        self.url = self.page.url
        self.timeouts = timeouts
        self.metadata = metadata

    @property
    def mouse(self):
        return BrowserMouse(
            self.page.mouse,
            self.timeouts,
            self.metadata,
            self.url
        )

    @property
    def frame(
        self,
        name: Optional[str] = None,
        url: Optional[str | Pattern[str] | Callable[[str], bool]]=None,
    ):
        command = FrameCommand(
            name=name,
            url=url
        )
        
        return BrowserFrame(
            self.page.frame(
                name=command.name,
                url=command.url
            ),
            self.timeouts,
            self.metadata
        )

    @property
    def frames(self):
        return [
            BrowserFrame(
                frame,
                self.timeouts,
                self.metadata
            ) for frame in self.page.frames
        ]

    async def add_script_tag(
        self,
        url: Optional[str] = None,
        path: Optional[str | Path] = None,
        content: Optional[str] = None,
        tag_type: Optional[str] = None,
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

        command = AddScriptTagCommand(
            url=url,
            path=path,
            content=content,
            tag_type=tag_type,
            timeout=timeout
        )

        result: ElementHandle = None
        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.page.add_script_tag(
                    url=command.url,
                    path=command.path,
                    content=command.content,
                    type=command.tag_type,
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='add_script_tag',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='add_script_tag',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
        
    async def add_init_script(
        self,
        script: Optional[str] = None,
        path: Optional[str | Path] = None,
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

        command = AddInitScriptCommand(
            script=script,
            path=path,
            timeout=timeout
        )

        result: ElementHandle = None
        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.page.add_init_script(
                    script=command.script,
                    path=command.path,
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='add_init_script',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='add_init_script',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
        
    async def add_locator_handler(
        self,
        locator: Locator,
        handler: Callable[
            [Locator],
            Any
        ] | Callable[[], Any],
        no_wait_after: Optional[bool] = None,
        times: Optional[int] = None,
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

        command = AddLocatorHandlerCommand(
            locator=locator,
            handler=handler,
            no_wait_after=no_wait_after,
            times=times,
            timeout=timeout
        )

        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.page.add_locator_handler(
                    command.locator,
                    command.handler,
                    no_wait_after=command.no_wait_after,
                    times=command.times

                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='add_locator_handler',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='add_locator_handler',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def add_style_tag(
        self,
        url: Optional[str] = None,
        path: Optional[str | Path] = None,
        content: Optional[str] = None,
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

        command = AddStyleTagCommand(
            url=url,
            path=path,
            content=content,
            timeout=timeout
        )

        result: ElementHandle = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.page.add_style_tag(
                    url=command.url,
                    path=command.path,
                    content=command.content

                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='add_style_tag',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='add_style_tag',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
        
    async def emulate_media(
        self,
        media: Optional[
            Literal['null', 'print', 'screen']
        ] = None,
        color_scheme: Optional[
            Literal['dark', 'light', 'no-preference', 'null']
        ] = None,
        reduced_motion: Optional[
            Literal['no-preference', 'null', 'reduce']
        ] = None,
        forced_colors: Optional[
            Literal['active', 'none', 'null']
        ] = None,
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

        command = EmulateMediaCommand(
            media=media,
            color_scheme=color_scheme,
            reduced_motion=reduced_motion,
            forced_colors=forced_colors,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.page.emulate_media(
                    media=command.media,
                    color_scheme=command.color_scheme,
                    reduced_motion=command.reduced_motion,
                    forced_colors=command.forced_colors
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='emulate_media',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='emulate_media',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
        
    async def expect_websocket(
        self,
        predicate: Optional[
            Callable[[WebSocket], bool]
        ],
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

        command = ExpectWebsocketCommand(
            predicate=predicate,
            timeout=timeout
        )

        response: WebSocket = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:
            websocket_response: AsyncEventContextManager[WebSocket] = await self.page.expect_websocket(
                predicate=command.predicate,
                timeout=command.timeout
            )

            async with websocket_response as websocket:
                response = websocket

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='expect_websocket',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='expect_websocket',
            command_args=command,
            metadata=self.metadata,
            result=response,
            timings=timings,
            url=self.url
        )
        
    async def expect_worker(
        self,
        predicate: Optional[
            Callable[[Worker], bool]
        ],
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

        command = ExpectWorkerCommand(
            predicate=predicate,
            timeout=timeout
        )

        response: Worker = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:
            websocket_response: AsyncEventContextManager[Worker] = await self.page.expect_worker(
                predicate=command.predicate,
                timeout=command.timeout
            )

            async with websocket_response as websocket:
                response = websocket

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='expect_worker',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='expect_worker',
            command_args=command,
            metadata=self.metadata,
            result=response,
            timings=timings,
            url=self.url
        )
        
    async def expose_binding(
        self,
        name: str,
        callback: Callable[..., Any],
        handle: Optional[bool] = None,
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

        command = ExposeBindingCommand(
            name=name,
            callback=callback,
            handle=handle,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.page.expose_binding(
                    command.name,
                    command.callback,
                    handle=command.handle,
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='expose_binding',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='expose_binding',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
        
    async def expose_function(
        self,
        name: str,
        callback: Callable[..., Any],
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

        command = ExposeFunctionCommand(
            name=name,
            callback=callback,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.page.expose_function(
                    command.name,
                    command.callback
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='expose_function',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='expose_function',
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
                self.page.frame_locator(
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
            timings=timings
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
                self.page.get_by_alt_text(
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
                self.page.get_by_label(
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
                self.page.get_by_placeholder(
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
                self.page.get_by_role(
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
                self.page.get_by_test_id(
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
                self.page.get_by_text(
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
                self.page.get_by_title(
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
        
    async def is_closed(
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

        command = IsClosedCommand(
            timeout=timeout
        )

        result: bool = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.page.is_closed(),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='is_closed',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='is_closed',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
        
    async def is_disabled(
        self,
        selector: str,
        strict: Optional[bool] = None,
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
            selector=selector,
            strict=strict,
            timeout=timeout
        )

        result: bool = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await self.page.is_disabled(
                command.selector,
                strict=command.strict,
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
        selector: str,
        strict: Optional[bool] = None,
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
            selector=selector,
            strict=strict,
            timeout=timeout
        )

        result: bool = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await self.page.is_editable(
                command.selector,
                strict=command.strict,
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

    async def opener(
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

        command = OpenerCommand(
            timeout=timeout
        )

        result: Optional[Page] = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.page.opener(),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='opener',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='opener',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
        
    async def on(
        self,
        event: Literal[
            'close',
            'console',
            'crash',
            'dialog',
            'domcontentloaded',
            'download',
            'filechooser',
            'frameattached',
            'framedetached',
            'framenavigated',
            'load',
            'pageerror',
            'popup',
            'request',
            'requestfailed',
            'requestfinished',
            'response',
            'websocket',
            'worker'
        ],
        function: Callable[
            [Page],
            Awaitable[None] | None
        ] |
        Callable[
            [ConsoleMessage],
            Awaitable[None] | None
        ] |
        Callable[
            [Page],
            Awaitable[None] | None
        ] |
        Callable[
            [Dialog],
            Awaitable[None] | None
        ] |
        Callable[
            [Page],
            Awaitable[None] | None
        ] |
        Callable[
            [Download],
            Awaitable[None] | None
        ] |
        Callable[
            [FileChooser],
            Awaitable[None] | None
        ] |
        Callable[
            [Frame],
            Awaitable[None] | None
        ] |
        Callable[
            [Frame],
            Awaitable[None] | None
        ] |
        Callable[
            [Frame],
            Awaitable[None] | None
        ] |
        Callable[
            [Page],
            Awaitable[None] | None
        ] |
        Callable[
            [Error],
            Awaitable[None] | None
        ] |
        Callable[
            [Page],
            Awaitable[None] | None
        ] |
        Callable[
            [Request],
            Awaitable[None] | None
        ] |
        Callable[
            [Request],
            Awaitable[None] | None
        ] |
        Callable[
            [Request],
            Awaitable[None] | None
        ] |
        Callable[
            [Response],
            Awaitable[None] | None
        ] |
        Callable[
            [WebSocket],
            Awaitable[None] | None
        ] | 
        Callable[
            [Worker],
            Awaitable[None] | None
        ],
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

        command = OnCommand(
            event=event,
            function=function,
            timeout=timeout
        )

        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            await self.page.on(
                command.event,
                command.function,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='on',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='on',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings
        )

    async def once(
        self,
        event: Literal[
            'close',
            'console',
            'crash',
            'dialog',
            'domcontentloaded',
            'download',
            'filechooser',
            'frameattached',
            'framedetached',
            'framenavigated',
            'load',
            'pageerror',
            'popup',
            'request',
            'requestfailed',
            'requestfinished',
            'response',
            'websocket',
            'worker'
        ],
        function: Callable[
            [Page],
            Awaitable[None] | None
        ] |
        Callable[
            [ConsoleMessage],
            Awaitable[None] | None
        ] |
        Callable[
            [Page],
            Awaitable[None] | None
        ] |
        Callable[
            [Dialog],
            Awaitable[None] | None
        ] |
        Callable[
            [Page],
            Awaitable[None] | None
        ] |
        Callable[
            [Download],
            Awaitable[None] | None
        ] |
        Callable[
            [FileChooser],
            Awaitable[None] | None
        ] |
        Callable[
            [Frame],
            Awaitable[None] | None
        ] |
        Callable[
            [Frame],
            Awaitable[None] | None
        ] |
        Callable[
            [Frame],
            Awaitable[None] | None
        ] |
        Callable[
            [Page],
            Awaitable[None] | None
        ] |
        Callable[
            [Error],
            Awaitable[None] | None
        ] |
        Callable[
            [Page],
            Awaitable[None] | None
        ] |
        Callable[
            [Request],
            Awaitable[None] | None
        ] |
        Callable[
            [Request],
            Awaitable[None] | None
        ] |
        Callable[
            [Request],
            Awaitable[None] | None
        ] |
        Callable[
            [Response],
            Awaitable[None] | None
        ] |
        Callable[
            [WebSocket],
            Awaitable[None] | None
        ] | 
        Callable[
            [Worker],
            Awaitable[None] | None
        ],
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

        command = OnCommand(
            event=event,
            function=function,
            timeout=timeout
        )

        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            await self.page.once(
                command.event,
                command.function,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='once',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='once',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def pause(
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

        command = PauseCommand(
            timeout=timeout
        )

        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.page.pause(),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='pause',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='pause',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
        
    async def pdf(
        self,
        scale: Optional[float] = None,
        display_header_footer: Optional[bool] = None,
        header_template: Optional[str]= None,
        footer_template: Optional[str] = None,
        print_background: Optional[bool] = None,
        landscape: Optional[bool] = None,
        page_ranges: Optional[str] = None,
        pdf_format: Optional[str] = None,
        width: Optional[str | float] = None,
        height: Optional[str | float] = None,
        prefer_css_page_size: Optional[bool] = None,
        margin: Optional[PdfMargins] = None,
        path: Optional[str | Path] = None,
        outline: Optional[bool] = None,
        tagged: Optional[bool] = None,
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

        command = PdfCommand(
            scale=scale,
            display_header_footer=display_header_footer,
            header_template=header_template,
            footer_template=footer_template,
            print_background=print_background,
            landscape=landscape,
            page_ranges=page_ranges,
            pdf_format=pdf_format,
            width=width,
            height=height,
            prefer_css_page_size=prefer_css_page_size,
            margin=margin,
            path=path,
            outline=outline,
            tagged=tagged,
            timeout=timeout
        )

        result: bytes = None
        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.page.pdf(
                    scale=command.scale,
                    display_header_footer=command.display_header_footer,
                    header_template=command.header_template,
                    footer_template=command.footer_template,
                    print_background=command.print_background,
                    landscape=command.landscape,
                    page_ranges=command.page_ranges,
                    pdf_format=command.pdf_format,
                    width=command.width,
                    height=command.height,
                    prefer_css_page_size=command.prefer_css_page_size,
                    margin=command.margin,
                    path=command.path,
                    outline=command.outline,
                    tagged=command.tagged
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='pdf',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='pdf',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
        
    async def remove_locator_handler(
        self,
        locator: Locator,
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

        command = RemoveLocatorHandlerCommand(
            locator=locator,
            timeout=timeout
        )

        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.page.remove_locator_handler(
                    command.locator
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='remove_locator_handler',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='remove_locator_handler',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
    async def route(
        self,
        url: str | Pattern[str] | Callable[[str], bool],
        handler: Callable[[Route], Any] | Callable[[Route, Request], Any],
        times: Optional[int] = None,
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

        command = RouteCommand(
            url=url,
            handler=handler,
            times=times,
            timeout=timeout
        )

        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.page.route(
                    command.url,
                    command.handler,
                    times=command.times
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='route',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='route',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings
        )

    async def route_from_har(
        self,
        har: Path | str,
        url: Optional[str | Pattern[str] | Callable[[str], bool]]=None,
        not_found: Optional[Literal['abort', 'fallback']] = None,
        update: Optional[bool] = None,
        update_content: Optional[Literal['attach', 'embed']] = None,
        update_mode: Optional[Literal['full', 'minimal']] = None,
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

        command = RouteFromHarCommand(
            har=har,
            url=url,
            not_found=not_found,
            update=update,
            update_content=update_content,
            update_mode=update_mode,
            timeout=timeout
        )

        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.page.route_from_har(
                    command.har,
                    url=command.url,
                    not_found=command.not_found,
                    update=command.update,
                    update_content=command.update_content,
                    update_mode=command.update_mode,
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='route_from_har',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='route_from_har',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
        
    async def set_content(
        self,
        html: str,
        wait_until: Optional[
            Literal[
                'commit', 
                'domcontentloaded', 
                'load', 
                'networkidle'
            ]
        ]=None,
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

        command = SetContentCommand(
            html=html,
            wait_until=wait_until,
            timeout=timeout
        )

        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            await self.page.set_content(
                command.html,
                wait_until=command.wait_until,
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
        
    async def set_input_files(
        self,
        selector: str,
        files: str | Path | FilePayload | Sequence[str | Path] | Sequence[FilePayload],
        strict: Optional[bool] = None,
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
            selector=selector,
            files=files,
            strict=strict,
            no_wait_after=no_wait_after,
            timeout=timeout
        )

        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            await self.page.set_input_files(
                command.selector,
                command.files,
                strict=command.strict,
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
            timings=timings
        )

    async def set_viewport_size(
        self,
        viewport_size: ViewportSize,
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

        command = SetViewportSize(
            viewport_size=viewport_size,
            timeout=timeout
        )

        err: Exception = None
        timings['command_start'] = time.monotonic()

        try:
            await asyncio.wait_for(
                self.page.set_viewport_size(
                    command.viewport_size
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='set_viewport_size',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='set_viewport_size',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
    
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
    
        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = GoToCommand(
            url=url,
            timeout=timeout,
            wait_util=wait_util,
            referrer=referrer
        )

        result: Optional[Response] = None
        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:
            result = await self.page.goto(
                command.url,
                timeout=command.timeout,
                wait_until=command.wait_util,
                referer=command.referrer
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()
            
            return PlaywrightResult(
                command='goto',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='goto',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings
        )

    async def get_url(
        self,
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

        command = GetUrlCommand(
            timeout=timeout
        )

        timings['command_start'] = time.monotonic()

        page_url = self.url  

        timings['command_end'] = time.monotonic() 

        return PlaywrightResult(
            command='get_url',
            command_args=command,
            metadata=self.metadata,
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
            selector=selector,
            value=value,
            no_wait_after=no_wait_after,
            strict=strict,
            force=force,
            timeout=timeout,
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.page.fill(
                command.selector,
                command.value,
                no_wait_after=command.no_wait_after,
                strict=command.strict,
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
            selector=selector,
            postion=postion,
            no_wait_after=no_wait_after,
            strict=strict,
            force=force,
            trial=trial,
            timeout=timeout,
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.page.check(
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
        timings['command_start'] = time.monotonic()

        try:
        
            await self.page.click(
                command.selector,
                position=command.postion,
                modifiers=command.modifiers,
                delay=command.delay,
                button=command.button,
                click_count=command.click_count,
                force=command.force,
                no_wait_after=command.no_wait_after,
                strict=command.strict,
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
        timings['command_start'] = time.monotonic()

        try:

            await self.page.dblclick(
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
            selector=selector,
            event_type=event_type,
            event_init=event_init,
            strict=strict,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.page.dispatch_event(
                command.selector,
                type=command.event_type,
                event_init=command.event_init,
                strict=command.strict,
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
        timings['command_start'] = time.monotonic()

        try:

            await self.page.drag_and_drop(
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

            return PlaywrightResult(
                command='drag_and_drop',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='drag_and_drop',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings
        )

    async def bring_to_front(
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

        command = BringToFrontCommand(
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await asyncio.wait_for(
                self.page.bring_to_front(),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='bring_to_front',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='bring_to_front',
            command_args=command,
            metadata=self.metadata,
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

        command = EvaluateOnSelectorCommand(
            selector=selector,
            expression=expression,
            arg=arg,
            strict=strict,
            timeout=timeout
        )

        result: Any = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.page.eval_on_selector(
                    command.selector,
                    command.expression,
                    arg=command.arg,
                    strict=command.strict,
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='eval_on_selector',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='eval_on_selector',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings
        )

    async def evaluate_on_selectors_all(
        self,
        selector: str,
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

        command = EvaluateOnSelectorCommand(
            selector=selector,
            expression=expression,
            arg=arg,
            timeout=timeout
        )

        result: Any = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.page.eval_on_selector_all(
                    command.selector,
                    command.expression,
                    arg=command.arg,
                    strict=command.strict,
                ),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='eval_on_selector_all',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='eval_on_selector_all',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings
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
                self.page.evaluate(
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
            timings=timings
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
                self.page.evaluate_handle(
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

        command = ExpectConsoleMessageCommand(
            predicate=predicate,
            timeout=timeout
        )

        result: ConsoleMessage = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            if command.predicate is None:
                message_response: AsyncEventContextManager[ConsoleMessage] = await self.page.expect_console_message(
                    timeout=command.timeout
                )

                async with message_response as message:
                    result = message

            else:
                message_response: AsyncEventContextManager[ConsoleMessage] = await self.page.expect_console_message(
                    predicate=command.predicate,
                    timeout=command.timeout
                )

                async with message_response as message:
                    result = message

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='expect_console_message',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='expect_console_message',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
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

        command = ExpectDownloadCommand(
            predicate=predicate,
            timeout=timeout
        )

        result: Download = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            if command.predicate is None:
                download_response: AsyncEventContextManager[Download] = await self.page.expect_download(
                    timeout=command.timeout
                )

                async with download_response as download:
                    result = download

            else:
                download_response: AsyncEventContextManager[Download] = await self.page.expect_download(
                    predicate=command.predicate,
                    timeout=command.timeout
                )

                async with download_response as download:
                    result = download

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='expect_download',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='expect_download',
            command_args=command,
            result=result,
            metadata=self.metadata,
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

        command = ExpectEventCommand(
            event=event,
            predicate=predicate,
            timeout=timeout
        )

        result: Any = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            if command.predicate is None:
                event_response: AsyncEventContextManager[Any] = await self.page.expect_event(
                    command.event,
                    timeout=command.timeout
                )

                async with event_response as event:
                    result = event

            else:
                event_response: AsyncEventContextManager[Any] = await self.page.expect_event(
                    command.event,
                    predicate=command.predicate,
                    timeout=command.timeout
                )

                async with event_response as event:
                    result = event

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='expect_event',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='expect_event',
            command_args=command,
            result=result,
            metadata=self.metadata,
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

        command = ExpectFileChooserCommand(
            predicate=predicate,
            timeout=timeout
        )

        result: Any = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            if command.predicate is None:
                file_chooser_response: AsyncEventContextManager[Any] = await self.page.expect_file_chooser(
                    timeout=command.timeout
                )

                async with file_chooser_response as file_chooser:
                    result = file_chooser

            else:
                file_chooser_response: AsyncEventContextManager[Any] = await  self.page.expect_file_chooser(
                    predicate=command.predicate,
                    timeout=command.timeout
                )

                async with file_chooser_response as file_chooser:
                    result = file_chooser

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='expect_file_chooser',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='expect_file_chooser',
            command_args=command,
            result=result,
            metadata=self.metadata,
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

        command = ExpectNavigationCommand(
            url=url,
            wait_until=wait_until,
            timeout=timeout
        )

        result: Response = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            navigation_response: AsyncEventContextManager[Response] = await self.page.expect_navigation(
                command.url,
                wait_until=command.wait_until,
                timeout=command.timeout
            )

            async with navigation_response as navigation:
                result = navigation

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='expect_navigation',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='expect_navigation',
            command_args=command,
            result=result,
            metadata=self.metadata,
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

        command = ExpectPopupCommand(
            predicate=predicate,
            timeout=timeout
        )

        result: Page = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            if command.predicate is None:
                popup_response: AsyncEventContextManager[Page] = await self.page.expect_popup(
                    timeout=command.timeout
                )

                async with popup_response as popup:
                    result = popup

            else:
                popup_response: AsyncEventContextManager[Page] = await self.page.expect_popup(
                    predicate=command.predicate,
                    timeout=command.timeout
                )

                async with popup_response as popup:
                    result = popup
        
        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='expect_popup',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='expect_popup',
            command_args=command,
            result=result,
            metadata=self.metadata,
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

        command = ExpectRequestCommand(
            url_or_predicate=url_or_predicate,
            timeout=timeout
        )

        result: Request = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            if command.url_or_predicate is None:
                response: AsyncEventContextManager[Request] = await self.page.expect_request(
                    timeout=command.timeout
                )

                async with response as resp:
                    result = resp

            else:
                response: AsyncEventContextManager[Request] = await self.page.expect_request(
                    url_or_predicate=command.url_or_predicate,
                    timeout=command.timeout
                )

                async with response as resp:
                    result = resp

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='expect_request',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='expect_request',
            command_args=command,
            result=result,
            metadata=self.metadata,
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

        command = ExpectRequestFinishedCommand(
            predicate=predicate,
            timeout=timeout
        )

        result: Request = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            if command.predicate is None:
                request: AsyncEventContextManager[Request] = await self.page.expect_request_finished(
                    timeout=command.timeout
                )

                async with request as req:
                    result = req

            else:
                request: AsyncEventContextManager[Request] = await self.page.expect_request_finished(
                    predicate=command.predicate,
                    timeout=command.timeout
                )

                async with request as req:
                    result = req

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='expect_request_finished',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='expect_request_finished',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
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

        command = ExpectResponseCommand(
            url_or_predicate=url_or_predicate,
            timeout=timeout
        )

        result: Response = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            if command.url_or_predicate is None:
                response: AsyncEventContextManager[Response] = await self.page.expect_response(
                    timeout=command.timeout
                )

                async with response as resp:
                    result = resp

            else:
                response: AsyncEventContextManager[Response] = await self.page.expect_response(
                    url_or_predicate=command.url_or_predicate,
                    timeout=command.timeout
                )

                async with response as resp:
                    result = resp
        
        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='expect_response',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='expect_response',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings
        )
    
    async def focus(
        self,
        selector: str,
        strict: Optional[bool]=False,
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
            selector=selector,
            strict=strict,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.page.focus(
                command.selector,
                strict=command.strict,
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
        timings['command_start'] = time.monotonic()

        try:

            await self.page.hover(
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
            timings=timings
        )
    
    async def inner_html(
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

        command = DOMCommand(
            selector=selector,
            strict=strict,
            timeout=timeout
        )

        result: str = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.inner_html(
                command.selector,
                strict=command.strict,
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
            timings=timings
        )

    async def inner_text(
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

        command = DOMCommand(
            selector=selector,
            strict=strict,
            timeout=timeout
        )

        result: str = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.inner_text(
                command.selector,
                strict=command.strict,
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
            timings=timings
        )

    async def input_value(
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

        command = DOMCommand(
            selector=selector,
            strict=strict,
            timeout=timeout
        )

        result: str = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.input_value(
                command.selector,
                strict=command.strict,
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
            selector=selector,
            key=key,
            delay=delay,
            no_wait_after=no_wait_after,
            strict=strict,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.page.press(
                command.selector,
                strict=command.strict,
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
            timings=timings
        )

    async def is_enabled(
        self,
        selector: str,
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

        command = DOMCommand(
            selector=selector,
            strict=strict,
            timeout=timeout
        )

        result: bool = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.is_enabled(
                command.selector,
                strict=command.strict,
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
            timings=timings
        )

    async def is_hidden(
        self,
        selector: str,
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

        command = DOMCommand(
            selector=selector,
            strict=strict,
            timeout=timeout
        )

        result: bool = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.is_hidden(
                command.selector,
                strict=command.strict,
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
            timings=timings
        )

    async def is_visible(
        self,
        selector: str,
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

        command = DOMCommand(
            selector=selector,
            strict=strict,
            timeout=timeout
        )

        result: bool = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.is_visible(
                command.selector,
                strict=command.strict,
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
            timings=timings
        )

    async def is_checked(
        self,
        selector: str,
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

        command = DOMCommand(
            selector=selector,
            strict=strict,
            timeout=timeout
        )

        result: bool = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.is_checked(
                command.selector,
                strict=command.strict,
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
            timings=timings
        )

    async def content(
        self,
        timeout: Optional[int | float]=None,
    ) -> PlaywrightResult[Exception] | PlaywrightResult[str]:
        
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

        command = ContentCommand(
            timeout=timeout
        )

        result: str = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.content(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='content',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='content',
            command_args=command,
            result=result,
            metadata=self.metadata,
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

        command = DOMCommand(
            selector=selector,
            strict=strict,
            timeout=timeout
        )
    
        result: Optional[ElementHandle] = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self.page.query_selector(
                    command.selector,
                    strict=command.strict
                ),
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='query_selector',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='query_selector',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
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
    
        command = DOMCommand(
            selector=selector,
            timeout=timeout
        )
    
        result: List[ElementHandle] = []
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.page.query_selector_all(command.selector),
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='query_selector_all',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='query_selector_all',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
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

        command = ReloadCommand(
            wait_util=wait_until,
            timeout=timeout
        )
    
        result: Optional[Response] = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.reload(
                wait_until=command.wait_util,
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='reload',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='reload',
            command_args=command,
            result=result,
            metadata=self.metadata,
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

            result = await self.page.screenshot(
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
        timings['command_start'] = time.monotonic()

        try:

            await self.page.select_option(
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
        timings['command_start'] = time.monotonic()

        try:

            await self.page.set_checked(
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
    
        command = SetTimeoutCommand(
            timeout=timeout
        )
    
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.page.set_default_timeout(
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='set_default_timeout',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='set_default_timeout',
            command_args=command,
            result=None,
            metadata=self.metadata,
            timings=timings,
            url=self.url
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
    
        command = SetTimeoutCommand(
            timeout=timeout
        )
    
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.page.set_default_navigation_timeout(
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='set_default_navigation_timeout',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='set_default_navigation_timeout',
            command_args=command,
            result=None,
            metadata=self.metadata,
            timings=timings,
            url=self.url
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

        command = SetExtraHTTPHeadersCommand(
            headers=headers,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await asyncio.wait_for(
                self.page.set_extra_http_headers(
                    command.headers
                ),
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='set_extra_http_headers',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='set_extra_http_headers',
            command_args=command,
            result=None,
            metadata=self.metadata,
            timings=timings,
            url=self.url
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
        timings['command_start'] = time.monotonic()

        try:

            await self.page.tap(
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
    
        command = DOMCommand(
            selector=selector,
            strict=strict,
            timeout=timeout
        )

        result: Optional[str]=None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.text_content(
                command.selector,
                strict=command.strict,
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
    
        command = TitleCommand(
            timeout=timeout
        )

        result: str=None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.page.title(),
                timeout=command.timeout
            )

        except Exception as err:
                
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='title',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='title',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings,
            url=self.url
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
    
        command = TypeCommand(
            selector=selector,
            text=text,
            delay=delay,
            no_wait_after=no_wait_after,
            strict=strict,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.page.type(
                    command.selector,
                    command.text,
                    delay=command.delay,
                    no_wait_after=command.no_wait_after,
                    strict=command.strict,
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
                timings=timings
            )
        
        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='type_text',
            command_args=command,
            result=None,
            metadata=self.metadata,
            timings=timings,
            url=self.url
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
            selector=selector,
            postion=postion,
            no_wait_after=no_wait_after,
            strict=strict,
            force=force,
            trial=trial,
            timeout=timeout,
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.page.uncheck(
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
        
        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = ExpectEventCommand(
            event=event,
            predicate=predicate,
            timeout=timeout,
        )

        result: Any = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.wait_for_event(
                event=command.event,
                predicate=command.predicate,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='wait_for_event',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='wait_for_event',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
        
    async def wait_for_function(
        self,
        expression: str,
        arg: Optional[Any] = None,
        polling: Optional[float | Literal['raf']] = None,
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

        command = WaitForFunctionCommand(
            expression=expression,
            arg=arg,
            polling=polling,
            timeout=timeout
        )

        result: JSHandle = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.wait_for_function(
                command.expression,
                arg=command.arg,
                polling=command.polling,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='wait_for_function',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='wait_for_function',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
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
        
        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = WaitForLoadStateCommand(
            state=state,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.page.wait_for_load_state(
                state=command.state,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='wait_for_load_state',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='wait_for_load_state',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
        )
        
    async def wait_for_selector(
        self,
        selector: str,
        state: Optional[Literal['attached', 'detached', 'hidden', 'visible']] = None,
        strict: Optional[bool] = None,
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

        command = WaitForSelectorCommand(
            selector=selector,
            state=state,
            strict=strict,
            timeout=timeout
        )

        result: Optional[ElementHandle] = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.wait_for_selector(
                command.selector,
                state=command.state,
                strict=command.strict,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='wait_for_selector',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='wait_for_selector',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
        
    async def wait_for_timeout(
        self,
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

        command = WaitForTimeoutCommand(
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.page.wait_for_timeout(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='wait_for_timeout',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='wait_for_timeout',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings,
            url=self.url
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
    
        timings: Dict[
            Literal[
                'command_start',
                'command_end'
            ],
            float
        ] = {}

        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = WaitForUrlCommand(
            url=url,
            wait_until=wait_until,
            timeout=timeout
        )

        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            await self.page.wait_for_url(
                command.url,
                wait_until=command.wait_until,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='wait_for_url',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='wait_for_url',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings
        )

    async def get_attribute(
        self,
        selector: str,
        name: str,
        strict: Optional[bool] = None,
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
            selector=selector,
            name=name,
            strict=strict,
            timeout=timeout,
        )

        result: Optional[str] = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.get_attribute(
                command.selector,
                command.name,
                strict=command.strict,
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
        
    async def go_back(
        self,
        wait_until: Optional[
            Literal[
                'commit', 
                'domcontentloaded', 
                'load', 
                'networkidle'
            ]
        ] = None,
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

        command = GoCommand(
            wait_until=wait_until,
            timeout=timeout,
        )

        result: Optional[Response] = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.go_back(
                wait_until=command.wait_until,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='go_back',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='go_back',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
        
    async def go_forward(
        self,
        wait_until: Optional[
            Literal[
                'commit', 
                'domcontentloaded', 
                'load', 
                'networkidle'
            ]
        ] = None,
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

        command = GoCommand(
            wait_until=wait_until,
            timeout=timeout,
        )

        result: Optional[Response] = None
        err: Optional[Exception] = None
        timings['command_start'] = time.monotonic()

        try:

            result = await self.page.go_forward(
                wait_until=command.wait_until,
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='go_forward',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings,
                url=self.url
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='go_forward',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings,
            url=self.url
        )
    
    async def locator(
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
                self.page.locator(
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
            timings=timings
        )

    async def close(
        self,
        run_before_unload: Optional[bool] = None, 
        reason: Optional[str] = None,
        timeout: Optional[int | float]=None
    ):
        if timeout is None:
            timeout = self.timeouts.request_timeout

        command = CloseCommand(
            run_before_unload=run_before_unload,
            reason=reason,
            timeout=timeout
        )

        await asyncio.wait_for(
            self.page.close(
                run_before_unload=command.run_before_unload,
                reason=command.reason
            ),
            timeout=command.timeout
        )