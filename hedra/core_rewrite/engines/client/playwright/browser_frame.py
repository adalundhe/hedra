import asyncio
import time
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Literal,
    Optional,
    Pattern,
)

from playwright.async_api import (
    ElementHandle,
    Frame,
    FrameLocator,
    JSHandle,
    Locator,
    Position,
    Response,
)

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .models.browser import BrowserMetadata
from .models.commands import (
    AddScriptTagCommand,
    AddStyleTagCommand,
    ContentCommand,
    DOMCommand,
    DragAndDropCommand,
    EvaluateCommand,
    FrameElementCommand,
    FrameLocatorCommand,
    GetByRoleCommand,
    GetByTestIdCommand,
    GetByTextCommand,
    GoToCommand,
    LocatorCommand,
    SetContentCommand,
    TitleCommand,
    WaitForFunctionCommand,
    WaitForLoadStateCommand,
    WaitForUrlCommand,
)
from .models.results import PlaywrightResult


class BrowserFrame:

    def __init__(
        self,
        frame: Frame,
        timeouts: Timeouts,
        metadata: BrowserMetadata
    ) -> None:
        self.frame = frame
        self.name = frame.name
        self.timeouts = timeouts
        self.metadata = metadata

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
                self.frame.add_script_tag(
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
                frame=self.name,
                result=err,
                error=str(err),
                timings=timings
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='add_script_tag',
            command_args=command,
            metadata=self.metadata,
            frame=self.name,
            result=result,
            timings=timings
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
                self.frame.add_style_tag(
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
                frame=self.name,
                result=err,
                error=str(err),
                timings=timings
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='add_style_tag',
            command_args=command,
            metadata=self.metadata,
            frame=self.name,
            result=result,
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

            result = await self.frame.content(
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='content',
                command_args=command,
                metadata=self.metadata,
                frame=self.name,
                result=err,
                error=str(err),
                timings=timings
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='content',
            command_args=command,
            result=result,
            metadata=self.metadata,
            frame=self.name,
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

            await self.frame.drag_and_drop(
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
                frame=self.name,
                result=err,
                error=str(err),
                timings=timings
            )

        timings['command_end'] = time.monotonic()

        
        return PlaywrightResult(
            command='drag_and_drop',
            command_args=command,
            metadata=self.metadata,
            frame=self.name,
            result=None,
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
                self.frame.evaluate(
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
                timings=timings
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
                self.frame.evaluate_handle(
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
                timings=timings
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='evaluate_handle',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings
        )
    
    async def frame_element(
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

        command = FrameElementCommand(
            timeout=timeout
        )

        result: ElementHandle = None
        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.frame.frame_element(),
                timeout=command.timeout
            )

        except Exception as err:
            
            timings['command_end'] = time.monotonic()

            return PlaywrightResult(
                command='frame_element',
                command_args=command,
                metadata=self.metadata,
                result=err,
                error=str(err),
                timings=timings
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='frame_element',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings
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
                self.frame.frame_locator(
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
                timings=timings
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
                self.frame.get_by_alt_text(
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
                timings=timings
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
                self.frame.get_by_label(
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
                timings=timings
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='get_by_label',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings
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
                self.frame.get_by_placeholder(
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
                timings=timings
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
                self.frame.get_by_role(
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
                timings=timings
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='get_by_role',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings
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
                self.frame.get_by_test_id(
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
                timings=timings
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
                self.frame.get_by_text(
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
                timings=timings
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
                self.frame.get_by_title(
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
                timings=timings
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='get_by_title',
            command_args=command,
            metadata=self.metadata,
            result=result,
            timings=timings
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
            result = await self.frame.goto(
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
                timings=timings
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='goto',
            command_args=command,
            metadata=self.metadata,
            result=result,
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

            result = await self.frame.is_enabled(
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
                timings=timings
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='is_enabled',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings
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
                self.frame.locator(
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
                timings=timings
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='locator',
            command_args=command,
            result=result,
            metadata=self.metadata,
            timings=timings
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
            await self.frame.set_content(
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
                timings=timings
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='set_content',
            command_args=command,
            metadata=self.metadata,
            result=None,
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
    
        command = TitleCommand(
            timeout=timeout
        )

        result: str=None
        err: Optional[Exception] = None

        timings['command_start'] = time.monotonic()

        try:

            result = await asyncio.wait_for(
                self.frame.title(),
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
            timings=timings
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

            result = await self.frame.wait_for_function(
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
                timings=timings
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='wait_for_function',
            command_args=command,
            metadata=self.metadata,
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

            await self.frame.wait_for_load_state(
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
                timings=timings
            )

        timings['command_end'] = time.monotonic()

        return PlaywrightResult(
            command='wait_for_load_state',
            command_args=command,
            metadata=self.metadata,
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

            await self.frame.wait_for_url(
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
                timings=timings
            )

        timings['command_end'] = time.monotonic()
        
        return PlaywrightResult(
            command='wait_for_url',
            command_args=command,
            metadata=self.metadata,
            result=None,
            timings=timings
        )