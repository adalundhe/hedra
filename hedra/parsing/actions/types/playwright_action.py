

class PlaywrightAction:

    def __init__(self, action, group=None):
        self.name = action.get('name')
        self.user = action.get('user', group)
        self.env = action.get('env')
        self.url = action.get('url')
        self.type = action.get('type')
        self.selector = action.get('selector')
        self.options = action.get('options', {})
        self.data = action.get('data', {})

        self.order = action.get('order', 1)
        self.weight = action.get('weight', 1)
        self.is_setup = action.get('is_setup', False)
        self.is_teardown = action.get('is_teardown', False)
        self.action_type = 'playwright'

    async def setup(self):
        pass

    @classmethod
    def about(cls):
        return '''
        Playwright Action

        Playwright actions in Hedra represent a single call to the Playwright API via Hedra's
        command library for Playwright. For example - a single click, inputing text, etc.

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

        For more information on supported Playwright actions and how to specify action data, 
        run the command:

            hedra --about engine:playwright:<command>

        '''
    async def execute(self, command):
        return await command(
            self
        )

    def to_dict(self):
        return {
            'name': self.name,
            'user': self.user,
            'env': self.env,
            'start_address': self.url,
            'type': self.type,
            'selector': self.selector,
            'options': self.options,
            'data': self.data,
            'weight': self.weight,
            'order': self.order,
            'action_type': self.action_type
        }