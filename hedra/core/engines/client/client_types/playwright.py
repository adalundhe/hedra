import asyncio
from types import FunctionType
from typing import Any, Dict, List, Callable, Union
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.types.playwright import (
    MercuryPlaywrightClient,
    PlaywrightCommand,
    PlaywrightResult,
    Page,
    URL,
    Input,
    Options
)
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.client.store import ActionsStore
from hedra.logging import HedraLogger
from .base_client import BaseClient


class PlaywrightClient(BaseClient[MercuryPlaywrightClient, PlaywrightCommand, PlaywrightResult]):

    def __init__(self, config: Config) -> None:
        super().__init__()
        
        self.session = MercuryPlaywrightClient(
            concurrency=config.batch_size,
            group_size=config.group_size,
            timeouts=Timeouts(
                total_timeout=config.request_timeout
            )
        )

        self.request_type = RequestTypes.PLAYWRIGHT
        self.client_type = self.request_type.capitalize()

        self.actions: ActionsStore = None
        self.next_name = None
        self.intercept = False

        self.logger = HedraLogger()
        self.logger.initialize()

    def __getitem__(self, key: str):
        return self.session.registered.get(key)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)
    
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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)

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

        return await self._execute_action(command)
