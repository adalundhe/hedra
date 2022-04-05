import random
from hedra.core.engines.types.helpers.playwright_helpers import command_librarian
from .base_engine import BaseEngine
from playwright.async_api import async_playwright
from async_tools.datatypes import AsyncDict, AsyncList
from .helpers.playwright_helpers import CommandLibrarian
from .utils.wrap_awaitable import async_execute_or_catch, wrap_awaitable_future


class PlaywrightEngine(BaseEngine):
    
    def __init__(self, config, handler):
        super().__init__(config, handler)
        self.page = None
        self.context = None
        self.browser = None
        self._device_type = self.config.get('device')
        self._browser_type = self.config.get('browser')
        self._locale = self.config.get('locale')
        self._geolocation = self.config.get('location')
        self._permissions = self.config.get('permissions')
        self._color_scheme = self.config.get('color_scheme')
        self._librarians = AsyncList()
        self._batch_size = self.config.get('batch_size')
        self.session = None
        
    @classmethod
    def about(cls):
        return '''
        Playwright Engine - (playwright)

        Playwright is an open source library for UI testing by Microsoft. Unlike Selenium, the Playwright 
        API in Python natively supports async, making it a significantly more stable and better choice for
        UI performance and load testing as calls are not synchronous. Hedra implements Playwright as an
        engine for testing UIs at scale, without requiring a Selenium grid or complex distributed setup.

        Actions are specified as:

        - name: <name_of_the_action>
        - user: <user_associated_with_the_action>
        - env: <environment_the_action_is_testing>
        - url: <base_address_of_target_ui_to_test>
        - type: <playwright_function_to_execute>
        - selector: <selector_for_playwright_call_to_use>
        - options: <dict_of_additional_options_for_playwright_call>
        - data: <dict_of_data_to_be_passed_in_playwright_call>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        The data parameter may contain the following options:

        - text: <text_for_input>
        - event: <name_of_dom_event_type>
        - from_coordinates: <dict_of_x_y_key_value_pairs_for_screen_position>
        - to_coordinates: <dict_of_x_y_key_value_pairs_for_screen_position>
        - function: <stringified_javascript_function_to_execute>
        - args: <optional_arguments_for_stringified_javascript_function>
        - key: <keyboard_key_to_input>
        - is_checked: <boolean_to_set_checked_if_true_uncheck_if_false>
        - timeout: <timeout_for_action>
        - headers: <headers_to_submit_with_request>
        - attribute: <attribute_of_dom_element_to_retrieve>
        - frame_selector: <select_frame_by_name_if_name_or_url_if_url>
        - option: <select_element_option_to_select>
        - state: <state_of_selector_or_page>
        - path: <path_to_file>

        The options parameter likewise offers helpful options including:

        - selector_type: <use_xpath_css_id_or_other_selector_type> (used in formatting)

        For more information on the Playwright Command Librarian, run the command:

            hedra:engines:playwright:librarian

        For more information on the Playwright Command Library, run the command:

            hedra:engine:playwright:library

        For a list of supported Playwright actions, run the command:

            hedra:engines:playwright:list

        For more information on how to make calls to a given Playwright action, run the command:

            hedra:engines:playwright:<playwright_function_name>

        '''

    @classmethod
    def about_librarian(cls):
        return CommandLibrarian.about()

    @classmethod
    def about_library(cls):
        return CommandLibrarian.about_library()

    @classmethod
    def about_registered_commands(cls):
        return CommandLibrarian.about_registered()

    @classmethod
    def about_command(cls, command):
        return CommandLibrarian.about_command(command)

    async def create_session(self, actions=AsyncList()):

        for action in actions.data:
            if action.is_setup:
                await action.execute()
            elif action.is_teardown:
                self._teardown_actions.append(action)

        self.session = await self.yield_session()

    async def yield_session(self):

        playwright = await async_playwright().start()

        if self._browser_type == "safari" or self._browser_type == "webkit":
            self.browser = await playwright.webkit.launch()

        elif self._browser_type == "firefox":
            self.browser = await playwright.firefox.launch()

        else:
            self.browser = await playwright.chromium.launch()


        config = AsyncDict()

        if self._device_type:
            device = playwright.devices[self._device_type]

            config = {
                **device
            }

        if self._locale:
            config['locale'] = self._locale

        if self._geolocation:
            config['geolocation'] = self._geolocation

        if self._permissions:
            config['permissions'] = self._permissions

        if self._color_scheme:
            config['color_scheme'] = self._color_scheme


        has_options = await config.size() > 0

        for _ in range(self._batch_size):

            if has_options:        
                context = await self.browser.new_context(
                    **config.data
                )
    
            else:
                context = await self.browser.new_context()
                

            page = await context.new_page()
            command_librarian = CommandLibrarian(page)

            await self._librarians.append(command_librarian)

    def execute(self, action):
        idx = random.randint(0, len(self._librarians))
        command = self._librarians[idx].get(action)
        return command(action)

    async def defer_all(self, actions):
        async for idx, action in actions.enum():
            async for command in self._librarians[idx].lookup(action):
                yield wrap_awaitable_future(
                    action,
                    command
                )

    @async_execute_or_catch()
    async def close(self):

        for teardown_action in self._teardown_actions:
            await self.execute(teardown_action)

        if self.session:
            await self.session.command_library.page.close()

        async for librarian in self._librarians:
            await librarian.command_library.page.close()
        await self.browser.close()
        