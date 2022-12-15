import uuid
from playwright.async_api import Page
from hedra.tools.helpers import awaitable
from .command import PlaywrightCommand


class CommandLibrary:

    __slots__ = (
        'page'
    )


    def __init__(self, page) -> None:
        self.page: Page = page

    async def goto(self, command: PlaywrightCommand):
        await self.page.goto(
            command.url.location, 
            **command.options.extra
        )

    async def get_url(self, command: PlaywrightCommand):
        return self.page.url

    async def fill(self, command: PlaywrightCommand):
        await self.page.fill(
            command.page.selector, 
            command.input.text
        )

    async def check(self, command: PlaywrightCommand):
        await self.page.check(
            command.page.selector, 
            **command.options.extra
        )

    async def click(self, command: PlaywrightCommand):
        await self.page.click(
            command.page.selector, 
            **command.options.extra
        )

    async def double_click(self, command: PlaywrightCommand):
        await self.page.dblclick(
            command.page.selector, 
            **command.options.extra
        )

    async def submit_event(self, command: PlaywrightCommand):
        await self.page.dispatch_event(
            command.page.selector, 
            command.options.event, 
            **command.options.extra
        )

    async def drag_and_drop(self, command: PlaywrightCommand):
        await self.page.drag_and_drop(
            source_position=command.page.x_coordinate, 
            target_position=command.page.y_coordinate,
            **command.options.extra
        )

    async def switch_active_tab(self, command: PlaywrightCommand):
        await self.page.bring_to_front()

    async def evaluate_selector(self, command: PlaywrightCommand):
        await self.page.eval_on_selector(
            command.page.selector, 
            command.input.expression, 
            *command.input.args, 
            **command.options.extra
        )

    async def evaluate_all_selectors(self, command: PlaywrightCommand):
        await self.page.eval_on_selector_all(
            command.page.selector,
            command.input.expression, 
            *command.input.args, 
            **command.options.extra
        )

    async def evaluate_expression(self, command: PlaywrightCommand):
        await self.page.evaluate(
            command.input.expression, 
            *command.input.args, 
            **command.options.extra
        )

    async def evaluate_handle(self, command: PlaywrightCommand):
        await self.page.evaluate_handle(
            command.input.expression, 
            *command.input.args, 
            **command.options.extra
        )

    async def exepect_console_message(self, command: PlaywrightCommand):
        await self.page.expect_console_message(
            predicate=await self.page.on(
                'console',
                command.input.expression
            ), 
            **command.options.extra
        )

    async def expect_download(self, command: PlaywrightCommand):
        await self.page.expect_download(
            predicate=await self.page.on(
                'download', 
                command.input.expression
            ), 
            **command.options.extra
        )

    async def expect_event(self, command: PlaywrightCommand):
        await self.page.expect_event(
            command.options.event, 
            **command.options.extra
        )

    async def expect_location(self, command: PlaywrightCommand):
        await self.page.expect_navigation(
            url=command.url.location, 
            **command.options.extra
        )

    async def expect_popup(self, command: PlaywrightCommand):
        await self.page.expect_popup(
            predicate=await self.page.on(
                'popup', 
                command.input.expression
            )
        )

    async def expect_request(self, command: PlaywrightCommand):
        return await self.page.expect_request(
            command.url.location, 
            **command.options.extra
        )

    async def expect_request_finished(self, command: PlaywrightCommand):
        await self.page.expect_request_finished(
            await self.expect_request(command)
        )

    async def expect_response(self, command: PlaywrightCommand):
        await self.page.expect_response(
            command.url.location, 
            **command.options.extra
        )

    async def focus(self, command: PlaywrightCommand):
        await self.page.focus(
            command.page.selector, 
            **command.options.extra
        )

    async def hover(self, command: PlaywrightCommand):
        await self.page.hover(
            command.page.selector, 
            **command.options.extra
        )

    async def get_inner_html(self, command: PlaywrightCommand):
        return await self.page.inner_html(
            command.page.selector, 
            **command.options.extra
        )

    async def get_text(self, command: PlaywrightCommand):
        return await self.page.inner_text(
            command.page.selector, 
            **command.options.extra
        )

    async def get_input_value(self, command: PlaywrightCommand):
        return await self.page.inner_value(
            command.page.selector, 
            **command.options.extra
        )

    async def press_key(self, command: PlaywrightCommand):
        await self.page.press(
            command.page.selector,
            command.input.key, 
            **command.options.extra
        )

    async def verify_is_enabled(self, command: PlaywrightCommand):
        return await self.page.is_enabled(
            command.page.selector, 
            **command.options.extra
        )

    async def verify_is_hidden(self, command: PlaywrightCommand):
        return await self.page.is_hidden(
            command.page.selector, 
            **command.options.extra
        )

    async def verify_is_visible(self, command: PlaywrightCommand):
        return await self.page.is_hidden(
            command.page.selector, 
            **command.options.extra
        )

    async def verify_is_checked(self, command: PlaywrightCommand):
        return await self.page.is_checked(
            command.page.selector, 
            **command.options.extra
        )

    async def get_content(self, command: PlaywrightCommand):
        return await self.page.content()

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