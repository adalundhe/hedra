import asyncio
import uuid
from typing import (
    Dict, 
    List, 
    Coroutine, 
    Any, 
    Callable,
    Union
)
from hedra.core.engines.types.common import Timeouts
from hedra.core_rewrite.engines.client.client_types.common.base_client import BaseClient
from hedra.core.engines.types.common.types import RequestTypes
from .context_config import ContextConfig
from .context_group import ContextGroup
from .pool import ContextPool
from .command import (
    PlaywrightCommand,
    Page,
    URL,
    Input,
    Options
)
from .result import PlaywrightResult


class PlaywrightClient(BaseClient[PlaywrightCommand, PlaywrightResult]):

    __slots__ = (
        'mutations',
        'actions',
        'next_name',
        'initialized',
        'suspend',
        'session_id',
        'pool',
        'timeouts',
        'registered',
        'closed',
        'config',
        'sem',
        'active',
        'waiter',
        '_discarded_context_groups',
        '_discarded_contexts',
        '_pending_context_groups',
        '_playwright_setup'
    )

    def __init__(self,  concurrency: int = 500, group_size: int=50, timeouts: Timeouts = Timeouts()) -> None:
        super(
            PlaywrightClient,
            self
        ).__init__()
        
        self.session_id = str(uuid.uuid4())
        self.next_name: Union[str, None] = None

        self.pool = ContextPool(concurrency, group_size)
        self.timeouts = timeouts
        self.registered: Dict[str, PlaywrightCommand] = {}
        self.closed = False
        self.config: Union[ContextConfig, None] = None

        self.sem = asyncio.Semaphore(value=concurrency)
        self.active = 0
        self.waiter = None
        self.initialized: bool = False
        self.suspend: bool = False

        self._discarded_context_groups: List[ContextGroup] = []
        self._discarded_contexts = []
        self._pending_context_groups: List[ContextGroup] = []
        self._playwright_setup = False
    
    def config_to_dict(self):
        return {
            'concurrency': self.pool.size,
            'group_size': self.pool.group_size,
            'timeouts': self.timeouts,
            'context_config': {
                **self.config.data,
                'options': self.config.options
            }
        }
        
    async def setup(self, config: ContextConfig=None):

        if config is None and self.config:
            config = self.config

        if self._playwright_setup is False:
            self.config = config
            self.pool.create_pool(self.config)
            for context_group in self.pool:
                await context_group.create()

            self._playwright_setup = True

    async def prepare(self, command: PlaywrightCommand) -> Coroutine[Any, Any, None]:

        command.options.extra = {
            **command.options.extra,
            'timeout': self.timeouts.total_timeout * 1000
        }

        self.registered[command.name] = command

    async def goto(
        self, 
        url: str, 
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'goto',
            url=URL(location=url),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def fill(
        self, 
        selector: str, 
        text: str,
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'fill',
            page=Page(selector=selector),
            input=Input(text=text),
            user=user,
            tags=tags,
            checks=checks     
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def check(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'check',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def click(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'click',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def double_click(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'double_click',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def submit_event(
        self,
        selector: str,
        event: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'double_click',
            page=Page(selector=selector),
            options=Options(
                event=event, 
                **extra
            ),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def drag_and_drop(
        self,
        x_coordinate: int,
        y_coordinate: int,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'drag_and_drop',
            page=Page(
                x_coordinate=x_coordinate,
                y_coordinate=y_coordinate
            ),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def switch_active_tab(
        self,
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'switch_active_tab',
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def evaluate_selector(
        self,
        selector: str,
        expression: str,
        args: List[Union[int, str, float, bool, None]]=[],
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'evaluate_selector',
            page=Page(selector=selector),
            input=Input(
                expression=expression,
                args=args
            ),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def evaluate_all_selectors(
        self,
        selector: str,
        expression: str,
        args: List[Union[int, str, float, bool, None]]=[],
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'evaluate_all_selectors',
            page=Page(selector=selector),
            input=Input(
                expression=expression,
                args=args
            ),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def evaluate_expression(
        self,
        expression: str,
        args: List[Union[int, str, float, bool, None]]=[],
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'evaluate_expression',
            input=Input(
                expression=expression,
                args=args
            ),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def evaluate_handle(
        self,
        expression: str,
        args: List[Union[int, str, float, bool, None]]=[],
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'evaluate_handle',
            input=Input(
                expression=expression,
                args=args
            ),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def exepect_console_message(
        self,
        expression: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'exepect_console_message',
            input=Input(expression=expression),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def expect_download(
        self,
        expression: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'expect_download',
            input=Input(expression=expression),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def expect_event(
        self,
        event: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'expect_event',
            options=Options(
                event=event, 
                **extra
            ),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def expect_location(
        self,
        url: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'expect_location',
            url=URL(location=url),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def expect_popup(
        self,
        expression: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'expect_popup',
            input=Input(expression=expression),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def expect_request(
        self,
        url: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'expect_request',
            url=URL(location=url),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def expect_request_finished(
        self,
        url: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'expect_request_finished',
            url=URL(location=url),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def expect_response(
        self,
        url: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'expect_response',
            url=URL(location=url),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def focus(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'focus',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def hover(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'hover',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def get_inner_html(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'get_inner_html',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def get_text(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'get_text',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def get_input_value(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'get_input_value',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def press_key(
        self,
        selector: str,
        key: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'press_key',
            page=Page(selector=selector),
            input=Input(key=key),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def verify_is_enabled(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'verify_is_enabled',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def verify_is_hidden(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'verify_is_hidden',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def verify_is_visible(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'verify_is_visible',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def verify_is_checked(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'verify_is_checked',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def get_content(
        self,
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'get_content',
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def get_element(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'get_element',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def get_all_elements(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'get_all_elements',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def reload_page(
        self,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'reload_page',
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def take_screenshot(
        self,
        screenshot_filepath: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'take_screenshot',
            input=Input(path=screenshot_filepath),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def select_option(
        self,
        selector: str,
        option: Union[str, int, bool, float, None],
        by_label: bool=False,
        by_value: bool=False,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'select_option',
            page=Page(selector=selector),
            input=Input(
                option=option,
                by_label=by_label,
                by_value=by_value
            ),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def set_checked(
        self,
        selector: str,
        checked: bool,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'set_checked',
            page=Page(selector=selector),
            options=Options(
                is_checked=checked,
                **extra
            ),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def set_default_timeout(
        self,
        timeout: float,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'set_default_timeout',
            options=Options(
                timeout=timeout,
                **extra
            ),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def set_navigation_timeout(
        self,
        timeout: float,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'set_navigation_timeout',
            options=Options(
                timeout=timeout,
                **extra
            ),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def set_http_headers(
        self,
        headers: Dict[str, str],
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'set_http_headers',
            url=URL(headers=headers),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def tap(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'tap',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def get_text_content(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'get_text_content',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def get_page_title(
        self,
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'get_page_title',
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def input_text(
        self,
        selector: str,
        text: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'input_text',
            page=Page(selector=selector),
            input=Input(text=text),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def uncheck(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'uncheck',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def wait_for_event(
        self,
        event: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'wait_for_event',
            options=Options(
                event=event,
                **extra
            ),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def wait_for_function(
        self,
        expression: str,
        args: List[Union[int, str, float, bool, None]]=[],
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'wait_for_function',
            input=Input(
                expression=expression,
                args=args
            ),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def wait_for_page_load_state(
        self,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'wait_for_page_load_state',
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def wait_for_selector(
        self,
        selector: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'wait_for_selector',
            page=Page(selector=selector),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def wait_for_timeout(
        self,
        timeout: float,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'wait_for_timeout',
            options=Options(
                timeout=timeout,
                **extra
            ),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)
    
    async def wait_for_url(
        self,
        url: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'wait_for_url',
            url=URL(location=url),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def switch_frame(
        self,
        url: str=None,
        frame: int=None,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'switch_frame',
            url=URL(location=url),
            page=Page(frame=frame),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def get_frames(
        self,
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'get_frames',
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def get_attribute(
        self,
        selector: str,
        attribute: str,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'get_attribute',
            page=Page(
                selector=selector,
                attribute=attribute
            ),
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def go_back_page(
        self,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'go_back_page',
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def go_forward_page(
        self,
        extra: Dict[str, Any]={}, 
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[Callable]=[]
    ):
        command = PlaywrightCommand(
            self.next_name,
            'go_forward_page',
            options=Options(**extra),
            user=user,
            tags=tags,
            checks=checks
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return command

        return await self.request(command)

    async def request(self, command: PlaywrightCommand) -> Coroutine[Any, Any, PlaywrightResult]:

        for pending_context in self._pending_context_groups:
            await pending_context.create()

        result = PlaywrightResult(command, type=RequestTypes.PLAYWRIGHT)
        self.active += 1
        
        async with self.sem:
            context = self.pool.contexts.pop()
            try:

                if command.hooks.listen:
                    event = asyncio.Event()
                    command.hooks.channel_events.append(event)
                    await event.wait()

                if command.hooks.before:
                    command = await self.execute_before(command)
            
                result = await context.execute(command)

                if command.hooks.after:
                    result = await self.execute_after(command, result)

                if command.hooks.notify:
                    await asyncio.gather(*[
                        asyncio.create_task(
                            channel.call(result, command.hooks.listeners)
                        ) for channel in command.hooks.channels
                    ])

                    for listener in command.hooks.listeners: 
                        if len(listener.hooks.channel_events) > 0:
                            event = listener.hooks.channel_events.pop()
                            if not event.is_set():
                                event.set()  

                self.pool.contexts.append(context)

            except Exception as e:
                result.error = e
                self.pool.contexts.append(context)

            self.active -= 1
            if self.waiter and self.active <= self.pool.size:

                try:
                    self.waiter.set_result(None)
                    self.waiter = None

                except asyncio.InvalidStateError:
                    self.waiter = None 

            return result

    async def close(self):
        if self.closed is False:
            for context_group in self._discarded_context_groups:
                await context_group.close()

            for context in self._discarded_contexts:
                await context.close()

            self.closed = True
                    