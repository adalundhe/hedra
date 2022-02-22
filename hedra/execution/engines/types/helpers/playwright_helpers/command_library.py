import uuid
from zebra_async_tools.functions import awaitable
from .registered_actions import registered_actions


class CommandLibrary:

    registered = registered_actions


    def __init__(self, page) -> None:
        self.page = page

    @classmethod
    def about(cls):
        return '''
        Playwright Command Library

        The Playwright command library is Hedra's base-layer integration with the Playwright API,
        providing a consistent interface for looking up and executing Playwright engine actions.

        For a list of supported Playwright actions, run the command:

            hedra:engines:playwright:list

        For more information on how to make calls to a given Playwright action, run the command:

            hedra:engines:playwright:<playwright_function_name>

        '''
        
    @classmethod
    def about_registered(cls):

        registered = '\n\t'.join([f'- {command}' for command in cls.registered])

        return f'''
        Playwright Command Library - Registered Commands

        Currently registered commands include:

        {registered}

        For more information on how to make calls to a given Playwright action, run the command:

            hedra:engines:playwright:<playwright_function_name>
        
        '''

    async def goto(self, action):
        await self.page.goto(action.url)

    async def get_url(self, action):
        return self.page.url

    async def fill(self, action):
        await self.page.fill(action.selector, action.data['text'])

    async def check(self, action):
        await self.page.check(action.selector, **action.options)

    async def click(self, action):
        await self.page.click(action.selector, **action.options)

    async def double_click(self, action):
        await self.page.dblclick(action.selector, **action.options)

    async def submit_event(self, action):
        await self.page.dispatch_event(action.selector, action.data['event'], **action.options)

    async def drag_and_drop(self, action):
        await self.page.drag_and_drop(
            source_position=action.data['from_coordinates'], 
            target_position=action.data['to_coordinates'],
            **action.options
        )

    async def switch_active_tab(self, action):
        await self.page.bring_to_front()

    async def evaluate_selector(self, action):
        await self.page.eval_on_selector(action.selector, action.data['function'], action.data['args'], **action.options)

    async def evaluate_all_selectors(self, action):
        await self.page.eval_on_selector_all(action.selector, action.data['function'], action.data['args'], **action.options)

    async def evaluate_function(self, action):
        await self.page.evaluate(action.data['function'], action.data['args'], **action.options)

    async def evaluate_handle(self, action):
        await self.page.evaluate_handle(action.data['function'], action.data['args'], **action.options)

    async def exepect_console_message(self, action):
        await self.page.expect_console_message(predicate=await self.page.on('console', action.data['function']), **action.options)

    async def expect_download(self, action):
        await self.page.expect_download(predicate=await self.page.on('download', action.data['function']), **action.options)

    async def expect_event(self, action):
        await self.page.expect_event(action.data['event'], **action.options)

    async def expect_location(self, action):
        await self.page.expect_navigation(url=action.url, **action.options)

    async def expect_popup(self, action):
        await self.page.expect_popup(predicate=await self.page.on('popup', action.data['function']))

    async def expect_request(self, action):
        return await self.page.expect_request(action.url, **action.options)

    async def expect_request_finished(self, action):
        await self.page.expect_request_finished(await self.expect_request(action))

    async def expect_response(self, action):
        await self.page.exepect_response(action.url, **action.options)

    async def focus(self, action):
        await self.page.focus(action.selector, **action.options)

    async def hover(self, action):
        await self.page.hover(action.selector, **action.options)

    async def get_inner_html(self, action):
        return await self.page.inner_html(action.selector, **action.options)

    async def get_text(self, action):
        return await self.page.inner_text(action.selector, **action.options)

    async def get_input_value(self, action):
        return await self.page.inner_value(action.selector, **action.options)

    async def press_key(self, action):
        await self.page.press(action.selector, action.data['key'], **action.options)

    async def verify_is_enabled(self, action):
        return await self.page.is_enabled(action.selector, **action.options)

    async def verify_is_hidden(self, action):
        return await self.page.is_hidden(action.selector, **action.options)

    async def verify_is_visible(self, action):
        return await self.page.is_hidden(action.selector, **action.options)

    async def verify_is_checked(self, action):
        return await self.page.is_checked(action.selector, **action.options)

    async def get_content(self, action):
        return await self.page.content()

    async def get_element(self, action):
        return await self.page.query_selector(action.selector, **action.options)

    async def get_all_elements(self, action):
        return await self.page.query_selector_all(action.selector, **action.options)

    async def reload_page(self, action):
        await self.page.reload(**action.options)

    async def take_screenshot(self, action):
        path = action.data['path']
        file_id = await awaitable(uuid.uuid4)
        await self.page.screenshot(path=f'{path}/{file_id}.png', **action.options)

    async def select_option(self, action):
        await self.page.select_option(action.selector, action.data['option'], **action.options)

    async def set_checked(self, action):
        await self.page.set_checked(action.selector, action.data['is_checked'], **action.options)

    async def set_default_timeout(self, action):
        await self.page.set_default_timeout(action.data['timeout'])

    async def set_navigation_timeout(self, action):
        await self.page.set_default_navigation_timeout(action.data)

    async def set_http_headers(self, action):
        await self.page.set_extra_http_headers(action.data['headers'])

    async def tap(self, action):
        await self.page.tap(action.selector, **action.options)

    async def get_text_content(self, action):
        await self.page.text_content(action.select, **action.options)

    async def get_page_title(self, action):
        await self.page.title()

    async def input_text(self, action):
        await self.page.type(action.selector, action.data['text'], **action.options)

    async def uncheck(self, action):
        await self.page.uncheck(action.selector, **action.options)

    async def wait_for_event(self, action):
        await self.page.wait_for_event(action.data['event'], **action.options)

    async def wait_for_function(self, action):
        await self.page.wait_for_function(action.data['function'], action.data['args'], **action.options)

    async def wait_for_page_load_state(self, action):
        await self.page.wait_for_load_state(**action.options)

    async def wait_for_selector(self, action):
        await self.page.wait_for_selector(action.selector, **action.options)

    async def wait_for_timeout(self, action):
        await self.page.wait_for_timeout(action.data['timeout'])

    async def wait_for_url(self, action):
        await self.page.wait_for_url(action.data['url'], **action.options)

    async def switch_frame(self, action):

        if action.url:
            await self.page.frame(url=action.url)

        else:
            await self.page.frame(name=action.data['frame'])

    async def get_frames(self, action):
        return await self.page.frames()

    async def get_attribute(self, action):
        return await self.page.get_attribute(action.selector, action.data['attribute'])

    async def go_back_page(self, action):
        await self.page.go_back(**action.options)

    async def go_forward_page(self, action):
        await self.page.go_forward(**action.options)