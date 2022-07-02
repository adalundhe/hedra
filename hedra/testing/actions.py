
from types import FunctionType
from typing import Any, Dict, List
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.playwright import (
    Command,
    Page,
    Input,
    URL,
    Options
)


class Action:
    session=None
    timeout=0
    is_setup = False
    is_teardown = False
    
    def __init__(self) -> None:
        self.data = None

    async def setup(self):
        self.is_setup = True


class HTTPAction(Action):

    def __init__(
        self, 
        name: str, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {}, 
        data: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType]=[]
    ) -> None:
        self.data = Request(
            name,
            url,
            method=method,
            headers=headers,
            params=params,
            payload=data,
            user=user,
            tags=tags,
            checks=checks
        )

    @classmethod
    def about(cls):
        return '''
        Mercury-HTTP Action

        Mercury-HTTP Actions represent a single HTTP 1.X REST call using Hedra's Mercury-HTTP
        engine. For example - a GET request to https://www.google.com/.

        Actions are specified as:

        - endpoint: <host_endpoint>
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - url: (optional) <full_target_url> (overrides host and endpoint if provided)
        - method: <rest_request_method>
        - headers: <rest_request_headers>
        - params: <rest_request_params>
        - data: <rest_request_data>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>

        '''

    async def setup(self):
        self.data.setup_http_request()
        await self.data.url.lookup()
        self.is_setup = True


class HTTP2Action(Action):

    def __init__(
        self, 
        name: str, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        data: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType]=[]
    ) -> None:
        self.data = Request(
            name,
            url,
            method=method,
            headers=headers,
            payload=data,
            user=user,
            tags=tags,
            checks=checks
        )

    @classmethod
    def about(cls):
        return '''
        Mercury-HTTP2 Action

        Mercury-HTTP2 Actions represent a single HTTP/2 REST call using Hedra's Mercury-HTTP2
        engine. For example - a GET request to https://www.google.com/.

        Actions are specified as:

        - endpoint: <host_endpoint>
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - url: (optional) <full_target_url> (overrides host and endpoint if provided)
        - method: <rest_request_method>
        - headers: <rest_request_headers>
        - params: <rest_request_params>
        - data: <rest_request_data>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>

        '''

    async def setup(self):
        self.data.setup_http2_request()
        await self.data.url.lookup()
        self.is_setup = True


class GraphQLAction(Action):

    def __init__(
        self, 
        name: str, 
        url: str, 
        query: str,
        operation_name: str = None,
        variables: Dict[str, Any] = None, 
        headers: Dict[str, str] = {}, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType]=[]
    ) -> None:
        self.data = Request(
            name,
            url,
            method='POST',
            headers=headers,
            payload={
                "query": query,
                "operation_name": operation_name,
                "variables": variables
            },
            user=user,
            tags=tags,
            checks=checks
        )

    @classmethod
    def about(cls):
        return '''
        Mercury-GraphQL Action

        Mercury-GraphQL Actions represent a single HTTP/1 or HTTP/2 REST call using Hedra's 
        Mercury-GraphQL engine. For example - a GET request to https://www.google.com/.

        Actions are specified as:

        - endpoint: <host_endpoint>
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - url: (optional) <full_target_url> (overrides host and endpoint if provided)
        - method: <rest_request_method>
        - headers: <rest_request_headers>
        - params: <rest_request_params>
        - data: <rest_request_data>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>

        '''

    async def setup(self):
        self.data.setup_graphql_request()
        await self.data.url.lookup()
        self.is_setup = True


class WebsocketAction(Action):
    
    def __init__(
        self, 
        name: str, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {}, 
        data: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType]=[]
    ) -> None:
        self.data = Request(
            name,
            url,
            method=method,
            headers=headers,
            params=params,
            payload=data,
            user=user,
            tags=tags,
            checks=checks
        )

    @classmethod
    def about(cls):
        return '''
        Mercury Websocket Action

        Mercury Websocket Actions represent a single cycle of connecting to, receiving/sending, and
        disconnecting from the websocket at the specified uri.

        Actions are specified as:

        - url: <full_url_to_target>
        - method: <webocket_request_method> (must be GET or POST)
        - headers: <websocket_request_headers>
        - params: <websocket_request_params>
        - data: <websocket_request_data>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        '''

    async def setup(self):
        self.data.setup_websocket_request()
        await self.data.url.lookup()
        self.is_setup = True


class PlaywrightAction(Action):

    def __init__(
        self,
        name: str,
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
        self.data = Command(
            name,
            command,
            page=Page(
                selector=selector,
                attribute=attribute,
                x_coordinate=x_coordinate,
                y_coordinate=y_coordinate,
                frame=frame
            ),
            url=URL(
                location=location,
                headers=headers
            ),
            input=Input(
                key=key,
                text=text,
                function=function,
                args=args,
                filepath=filepath,
                file=file
            ),
            options=Options(
                event=event,
                option=option,
                is_checked=is_checked,
                timeout=timeout,
                extra=extra,
                switch_by=switch_by
            ),
            user=user,
            tags=tags,
            checks=checks
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

    async def setup(self):
        self.is_setup = True


class GRPCAction(Action):

    def __init__(
        self, 
        name: str, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        protobuf: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType]=[]
    ) -> None:
        self.data = Request(
            name,
            url,
            method=method,
            headers=headers,
            payload=protobuf,
            user=user,
            tags=tags,
            checks=checks
        )

    @classmethod
    def about(cls):
        return '''
        Mercury-GRPC Action

        Mercury-GRPC Actions represent a single GRPC call using Hedra's Mercury-GRPC engine.

        Actions are specified as:

        - endpoint: <host_endpoint>
        - url: <full_url_to_target>
        - headers: <rest_request_headers>
        - data: <grpc_protobuf_object>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        '''

    async def setup(self):
        self.data.setup_grpc_request()
        await self.data.url.lookup()
        self.is_setup = True