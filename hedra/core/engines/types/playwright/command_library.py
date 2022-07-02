import uuid
from async_tools.functions import awaitable
from .command import Command


class CommandLibrary:


    def __init__(self, page) -> None:
        self.page = page

    async def goto(self, command: Command):
        await self.page.goto(
            command.url.location, 
            **command.options.extra
        )

    async def get_url(self, command: Command):
        return self.page.url

    async def fill(self, command: Command):
        await self.page.fill(
            command.page.selector, 
            command.input.text
        )

    async def check(self, command: Command):
        await self.page.check(
            command.page.selector, 
            **command.options.extra
        )

    async def click(self, command: Command):
        await self.page.click(
            command.page.selector, 
            **command.options.extra
        )

    async def double_click(self, command: Command):
        await self.page.dblclick(
            command.page.selector, 
            **command.options.extra
        )

    async def submit_event(self, command: Command):
        await self.page.dispatch_event(
            command.page.selector, 
            command.options.event, 
            **command.options.extra
        )

    async def drag_and_drop(self, command: Command):
        await self.page.drag_and_drop(
            source_position=command.page.x_coordinate, 
            target_position=command.page.y_coordinate,
            **command.options.extra
        )

    async def switch_active_tab(self, command: Command):
        await self.page.bring_to_front()

    async def evaluate_selector(self, command: Command):
        await self.page.eval_on_selector(
            command.page.selector, 
            command.input.function, 
            *command.input.args, 
            **command.options.extra
        )

    async def evaluate_all_selectors(self, command: Command):
        await self.page.eval_on_selector_all(
            command.page.selector,
            command.input.function, 
            *command.input.args, 
            **command.options.extra
        )

    async def evaluate_function(self, command: Command):
        await self.page.evaluate(
            command.input.function, 
            *command.input.args, 
            **command.options.extra
        )

    async def evaluate_handle(self, command: Command):
        await self.page.evaluate_handle(
            command.input.function, 
            *command.input.args, 
            **command.options.extra
        )

    async def exepect_console_message(self, command: Command):
        await self.page.expect_console_message(
            predicate=await self.page.on(
                'console',
                command.input.function
            ), 
            **command.options.extra
        )

    async def expect_download(self, command: Command):
        await self.page.expect_download(
            predicate=await self.page.on(
                'download', 
                command.input.function
            ), 
            **command.options.extra
        )

    async def expect_event(self, command: Command):
        await self.page.expect_event(
            command.options.event, 
            **command.options.extra
        )

    async def expect_location(self, command: Command):
        await self.page.expect_navigation(
            url=command.url.location, 
            **command.options.extra
        )

    async def expect_popup(self, command: Command):
        await self.page.expect_popup(
            predicate=await self.page.on(
                'popup', 
                command.input.function
            )
        )

    async def expect_request(self, command: Command):
        return await self.page.expect_request(
            command.url.location, 
            **command.options.extra
        )

    async def expect_request_finished(self, command: Command):
        await self.page.expect_request_finished(
            await self.expect_request(command)
        )

    async def expect_response(self, command: Command):
        await self.page.exepect_response(
            command.url.location, 
            **command.options.extra
        )

    async def focus(self, command: Command):
        await self.page.focus(
            command.page.selector, 
            **command.options.extra
        )

    async def hover(self, command: Command):
        await self.page.hover(
            command.page.selector, 
            **command.options.extra
        )

    async def get_inner_html(self, command: Command):
        return await self.page.inner_html(
            command.page.selector, 
            **command.options.extra
        )

    async def get_text(self, command: Command):
        return await self.page.inner_text(
            command.page.selector, 
            **command.options.extra
        )

    async def get_input_value(self, command: Command):
        return await self.page.inner_value(
            command.page.selector, 
            **command.options.extra
        )

    async def press_key(self, command: Command):
        await self.page.press(
            command.page.selector,
            command.input.key, 
            **command.options.extra
        )

    async def verify_is_enabled(self, command: Command):
        return await self.page.is_enabled(
            command.page.selector, 
            **command.options.extra
        )

    async def verify_is_hidden(self, command: Command):
        return await self.page.is_hidden(
            command.page.selector, 
            **command.options.extra
        )

    async def verify_is_visible(self, command: Command):
        return await self.page.is_hidden(
            command.page.selector, 
            **command.options.extra
        )

    async def verify_is_checked(self, command: Command):
        return await self.page.is_checked(
            command.page.selector, 
            **command.options.extra
        )

    async def get_content(self, command: Command):
        return await self.page.content()

    async def get_element(self, command: Command):
        return await self.page.query_selector(
            command.page.selector, 
            **command.options.extra
        )

    async def get_all_elements(self, command: Command):
        return await self.page.query_selector_all(
            command.page.selector, 
            **command.options.extra
        )

    async def reload_page(self, command: Command):
        await self.page.reload(**command.options.extra)

    async def take_screenshot(self, command: Command):
        file_id = await awaitable(uuid.uuid4)
        await self.page.screenshot(
            path=f'{command.input.path}/{file_id}.png', 
            **command.options.extra
        )

    async def select_option(self, command: Command):
        await self.page.select_option(
            command.page.selector, 
            command.options.option, 
            **command.options.extra
        )

    async def set_checked(self, command: Command):
        await self.page.set_checked(
            command.page.selector, 
            command.options.is_checked, 
            **command.options.extra
        )

    async def set_default_timeout(self, command: Command):
        await self.page.set_default_timeout(command.options.timeout)

    async def set_navigation_timeout(self, command: Command):
        await self.page.set_default_navigation_timeout(command.options.timeout)

    async def set_http_headers(self, command: Command):
        await self.page.set_extra_http_headers(command.url.headers)

    async def tap(self, command: Command):
        await self.page.tap(
            command.page.selector, 
            **command.options.extra
        )

    async def get_text_content(self, command: Command):
        await self.page.text_content(
            command.page.selector,
            **command.options.extra
        )

    async def get_page_title(self, command: Command):
        await self.page.title()

    async def input_text(self, command: Command):
        await self.page.type(
            command.page.selector,
            command.input.text, 
            **command.options.extra
        )

    async def uncheck(self, command: Command):
        await self.page.uncheck(
            command.page.selector, 
            **command.options.extra
        )

    async def wait_for_event(self, command: Command):
        await self.page.wait_for_event(
            command.options.event, 
            **command.options.extra
        )

    async def wait_for_function(self, command: Command):
        await self.page.wait_for_function(
            command.input.function, 
            *command.input.args, 
            **command.options.extra
        )

    async def wait_for_page_load_state(self, command: Command):
        await self.page.wait_for_load_state(**command.options.extra)

    async def wait_for_selector(self, command: Command):
        await self.page.wait_for_selector(
            command.page.selector, 
            **command.options.extra
        )

    async def wait_for_timeout(self, command: Command):
        await self.page.wait_for_timeout(command.options.timeout)

    async def wait_for_url(self, command: Command):
        await self.page.wait_for_url(
            command.url.location, 
            **command.options.extra
        )

    async def switch_frame(self, command: Command):

        if command.options.switch_by == 'url':
            await self.page.frame(url=command.url.location)

        else:
            await self.page.frame(name=command.page.frame)

    async def get_frames(self, command: Command):
        return await self.page.frames()

    async def get_attribute(self, command: Command):
        return await self.page.get_attribute(
            command.page.selector,
            command.page.attribute
        )

    async def go_back_page(self, command: Command):
        await self.page.go_back(**command.options.extra)

    async def go_forward_page(self, command: Command):
        await self.page.go_forward(**command.options.extra)