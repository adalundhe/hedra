from types import FunctionType
from typing import Any, Dict, List
from hedra.core.engines.types.playwright import (
    Command,
    Page,
    URL,
    Input,
    Options
)
from .base import Action


class PlaywrightAction(Action):

    def __init__(
        self,
        command: str,
        selector: str=None,
        attribute: str=None,
        x_coordinate: int=0,
        y_coordinate: int=0,
        frame: int=0,
        location: str=None,
        headers: Dict[str, str]={},
        key: str=None,
        text: str=None,
        function: str=None,
        args: List[Any]=None,
        filepath: str=None,
        file: bytes=None,
        event: str=None,
        option: str=None,
        is_checked: bool=False,
        timeout: int=60000,
        extra: Dict[str, Any]={},
        switch_by: str='url',
        user: str=None,
        tags: List[Dict[str, str]]=[],
        checks: List[FunctionType] = []
    ) -> None:
        super().__init__()
        self.command = command
        self.selector = selector
        self.attribute = attribute
        self.x_coordinate = x_coordinate
        self.y_coordinate = y_coordinate
        self.frame = frame
        self.location = location
        self.headers = headers
        self.key = key
        self.text = text
        self.function = function
        self.args = args
        self.filepath = filepath
        self.file = file
        self.event = event
        self.option = option
        self.is_checked = is_checked
        self.timeout = timeout
        self.extra = extra
        self.switch_by = switch_by
        self.user = user
        self.tags = tags
        self.checks = checks

    def to_type(self, name: str):
        self.parsed = Command(
            name,
            self.command,
            page=Page(
                selector=self.selector,
                attribute=self.attribute,
                x_coordinate=self.x_coordinate,
                y_coordinate=self.y_coordinate,
                frame=self.frame
            ),
            url=URL(
                location=self.location,
                headers=self.headers
            ),
            input=Input(
                key=self.key,
                text=self.text,
                function=self.function,
                args=self.args,
                filepath=self.filepath,
                file=self.file
            ),
            options=Options(
                event=self.event,
                option=self.option,
                is_checked=self.is_checked,
                timeout=self.timeout,
                extra=self.extra,
                switch_by=self.switch_by
            ),
            user=self.user,
            tags=self.tags,
            checks=self.checks,
            hooks=self.hooks
        )

    @classmethod
    def about(cls):
        return '''
        Playwright Action

        Playwright actions in Hedra represent a single call to the Playwright API via Hedra's
        command library for Playwright. For example - a single click, inputing text, etc.

        Actions are specified as:

        - name: <name_of_the_action>
        - command: <name_of_command_to_execute>
        - checks: <list_of_async_python_functions_with_MercuryHTTP_response/result_and_action_exection_time_as_args>

        Page:
        - selector: <selector_for_playwright_call_to_use>
        - attribute: <html_attribute_to_select_or_use>
        - x_coordinate: <x_coordinate_page_position>
        - y_coordinate: <y_coordinate_page_position>
        - frame <frame_index_to_use>

        URL:
        - location: <url_location_to_navigate_to_or_use>
        - headers: <headers_to_use_in_API_requests>

        Input:
        - key: <key_to_press>
        - text: <text_to_input>
        - function: <JavaScript_function_to_execute>
        - args: <list_of_args_for_JS_function>
        - filepath: <path_to_file_for_upload_or_save>
        - file: <raw_file_bytes_to_upload>

        Options
        - event: <event_name_to_listen_for>
        - option: <select_html_element_option_to_select>
        - is_checked: <whether_a_checkbox_element_should_be_checked>
        - timeout: <timeout_for_playwright_call (default is 60sec.)>
        - extra: <dictionary_of_arbitrary_additional_args>
        - switch_by: <whether_to_switch_frame_by_url_or_frame_index (default is url)>

        Metadata:
        - user: <user_associated_with_the_action>
        - tags: <list_of_dicts_of_key_value_string_pairs>

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

        For more information on supported Playwright actions and how to specify action data, 
        run the command:

            hedra --about engine:playwright:<command>

        '''