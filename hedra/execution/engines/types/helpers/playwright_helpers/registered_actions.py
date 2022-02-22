registered_actions = {
        'goto': '''
        Playwright Command - Goto

        Goes to url specified by action's url attribute.

        ''',
        'get_url': '''
        Playwright Command - Get Url

        Returns current url of page.

        ''',
        'fill': '''
        Playwright Command - Fill

        Finds input element specified by action selector attribute and inputs text 
        specified by action's data attribute under the dictionary key "text" - i.e.:

        action.data['text']
        
        ''',
        'check': '''
        Playwright Command - Check

        Finds the specified radio checkbox element using the selector specified by
        the action's selector attribute and selects it.
        
        ''',
        'get_content': '''
        Playwright Command - Get Content

        Retrieves all HTML content on page including doctype.
        
        ''',
        'click': '''
        Playwright Command - Click

        Finds the specified element using the selector specified by the action's selector 
        attribute and clicks it.

        ''',
        'double_click': '''
        Playwright Command - Double Click

        Finds the specified element using the selector specified by the action's selector 
        attribute and double clicks it.
        
        ''',
        'submit_event': '''
        Playwright Command - Submit Event

        Finds the specified element using the selector specified by the action's selector
        and performs the event specified by action's data attribute under the dictionary 
        key "event" - i.e.:

        action.data['event']

        Example valid events include:
        -click
        -mouseenter
        -mouseleave
        -hover
        
        ''',
        'drag_and_drop': '''
        Playwright Command - Drag and Drop

        Finds the element at the X/Y coordinates specified by the action's data attribute
        under the dictionary 'from_coordinates' key:

        action.data['from_coordinates']

        to the X/Y coordinates specified by the action's data attribute under the dictionary
        'to_coordinates' key:

        action.data['to_coordinates']

        Coordinates for the 'from_coordinates' and 'to_coordinates' keys should be supplied
        as dictionaries of the following format:

        {
            "x": <x_coordinate_number>,
            "y": <y_coordinate_number>
        }
        
        ''',
        'evaluate_selector': '''
        Playwright Command - Evaluate Selector

        Finds the specified element using the selector specified by the action's selector attribute 
        and passes it to the JavaScript function specified by the action's data attribute under the 
        dictionary 'function' key (along with any additional arguments specified under the action's 
        data attribute under the dictionary 'args' key):

        action.data['function']

        action.data['args']
        
        ''',
        'evaluate_all_selectors': '''
        Playwright Command - Evaluate All Selectors

        Finds all elements matching the selector specified by the action's selector attribute 
        and passes them to the JavaScript function specified by the action's data attribute under 
        the dictionary 'function' key (along with any additional arguments specified under the 
        action's data attribute under the dictionary 'args' key):

        action.data['function']

        action.data['args']
        
        ''',
        'evaluate_function': '''
        Playwright Command - Evaluate Function

        Executes the JavaScript function specified by the action's data attribute under the dictionary 'function' 
        key (along with any additional arguments specified under the action's data attribute under the dictionary 
        'args' key):

        action.data['function']

        action.data['args']
        
        ''',
        'evaluate_handle': '''
        Playwright Command - Evaluate Handle

        Executes the JavaScript expression specified by the action's data attribute under the dictionary 'function' 
        key (along with any additional arguments specified under the action's data attribute under the dictionary 
        'args' key):

        action.data['function']

        action.data['args']

        ''',
        'expect_console_message': '''
        Playwright Command - Expect Console Message

        Waits for the a console log event to execute, then executes the JavaScript function specified by the 
        action's data attribute under the dictionary 'function' key (along with any additional arguments specified 
        under the action's data attribute under the dictionary 'args' key):

        action.data['function']

        action.data['args']

        ''',
        'expect_download': '''
        Playwright Command - Expect Download

        Waits for the a file/object to download event to execute, then executes the JavaScript function specified 
        by the action's data attribute under the dictionary 'function' key (along with any additional arguments 
        specified under the action's data attribute under the dictionary 'args' key):

        action.data['function']

        action.data['args']

        ''',
        'expect_event': '''
        Playwright Command - Expect Event

        Waits for the specified DOM event to execute, then executes the JavaScript function specified by the action's 
        data attribute under the dictionary 'function' key (along with any additional arguments specified under the 
        action's data attribute under the dictionary 'args' key):

        action.data['function']

        action.data['args']

        ''',
        'expect_location': '''
        Playwright Command - Expect Location

        Waits for navigation to the specified url to finish, then executes the JavaScript function specified 
        by the action's data attribute under the dictionary 'function' key (along with any additional arguments 
        specified under the action's data attribute under the dictionary 'args' key):

        action.data['function']

        action.data['args']

        ''',
        'expect_popup': '''
        Playwright Command - Expect Popup

        Waits the a popup event to occur, then executes the JavaScript function specified by the action's 
        data attribute under the dictionary 'function' key (along with any additional arguments specified under 
        the action's data attribute under the dictionary 'args' key):

        action.data['function']

        action.data['args']
        
        ''',
        'expect_request': '''
        Playwright Command - Expect Request

        Waits for a request to the url specified by the action's url attribute to occur, then executes the JavaScript 
        function specified by the action's data attribute under the dictionary 'function' key (along with any additional 
        arguments specified under the action's data attribute under the dictionary 'args' key):

        action.data['function']

        action.data['args']
        
        ''',
        'expect_request_finished': '''
        Playwright Command - Expect Request

        Waits for a request to the url specified by the action's url attribute to occur, then executes the JavaScript 
        function specified by the action's data attribute under the dictionary 'function' key (along with any additional 
        arguments specified under the action's data attribute under the dictionary 'args' key):

        action.data['function']

        action.data['args']
        
        ''',
        'expect_response': '''
        Playwright Command - Expect Response

        Waits for a request to the url specified by the action's url attribute to return a response, then executes the 
        JavaScript function specified by the action's data attribute under the dictionary 'function' key (along with 
        any additional arguments specified under the action's data attribute under the dictionary 'args' key):

        action.data['function']

        action.data['args']
        
        ''',
        'focus': '''
        Playwright Command - Focus

        Finds the specified element using the selector specified by the action's selector 
        attribute and focuses on the element.
        
        ''',
        'hover': '''
        Playwright Command - Hover

        Finds the specified element using the selector specified by the action's selector 
        attribute and hovers over the element.
        
        ''',
        'get_inner_html': '''
        Playwright Command - Get Inner HTML

        Finds the specified element using the selector specified by the action's selector 
        attribute and returns the HTML of all child elements to the found element.
        
        ''',
        'get_text': '''
        Playwright Command - Get Text

        Finds the specified element using the selector specified by the action's selector 
        attribute and returns all text inside the element.

        ''',
        'get_input_value': '''
        Playwright Command - Get Input Value

        Finds the input specified element using the selector specified by the action's selector 
        attribute and returns the current value of the input.
        
        ''',
        'press_key': '''
        Playwright Command - Press Key

        Finds the specified element using the selector specified by the action's selector 
        attribute and inputs the key specified under the action's data attribute under the
        dictionary 'key' key:

        action.data['key']
        
        ''',
        'verify_is_checked': '''
        Playwright Command - Verify Is Checked

        Finds the specified element using the selector specified by the action's selector
        attribute and returns whether the element is checked. Note that the element must be
        a checkbox or radio button or Playwright will throw and error.
        
        ''',
        'verify_is_visible': '''
        Playwright Command - Verify Is Visible

        Finds the specified element using the selector specified by the action's selector
        attribute and returns whether the element is visible.

        ''',
        'verify_is_hidden': '''
        Playwright Command - Verify Is Hidden

        Finds the specified element using the selector specified by the action's selector
        attribute and returns whether the element is hidden.
        
        ''',
        'verify_is_enabled': '''
        Playwright Command - Verify Is Enabled

        Finds the specified element using the selector specified by the action's selector
        attribute and returns whether the element is enabled.
        
        ''',
        'get_element': '''
        Playwright Command - Get Element

        Finds the specified element using the selector specified by the action's selector
        attribute and returns it.
        
        ''',
        'get_all_elements': '''
        Playwright Command - Get All Elements

        Finds all elements matching the selector specified by the action's selector attribute 
        and returns it.
        
        ''',
        'reload_page': '''
        Playwright Command - Reload Page

        Reloads the current page.

        ''',
        'take_screenshot': '''
        Playwright Command - Take Screenshot

        Takes screenshot of the current page, saving it at the path specified under the 'path'
        key of the dictionary under the action's data attribute.
        
        ''',
        'select_option': '''
        Playwright Command - Select Option

        Finds the select slement specifed by the action's selector attribute and selects the option
        specified by the action's data attribute under the dictionary 'option' key:

        action.data['option']
        
        ''',
        'set_checked': '''
        Playwright Command - Set Checked

        Finds the checkbox or radio element specified by the action's selector attribute and checks
        or unchecks the element based on whether the balue for the 'is_checked' key for the dictionary 
        under the action's data attribute is true (checked) or false (unchecked):

        action.data['is_checked']
        
        ''',
        'set_default_timeout': '''
        Playwright Command - Set Default Timeout

        Sets the default timeout for all Playwright API calls based on the value for the 'timeout' key
        under the action's data attribute.

        action.data['timeout']
        
        ''',
        'set_navigation_timeout': '''
        Playwright Command - Set Navigation Timeout

        Sets the navigation timeout for all page navigation and page loading based on the value for the 
        'timeout' key under the action's data attribute.

        action.data['timeout']
        
        ''',
        'set_http_headers': '''
        Playwright Command - Set HTTP Headers

        Sets the http headers for all HTTP requests made by Playwright API calls based on the value 
        under the 'headers' key for the dictionary under the action's data attribute.

        action.data['headers']
        
        ''',
        'tap': '''
        Playwright Command - Tap

        Finds the select slement specifed by the action's selector attribute and "taps" it.
        
        ''',
        'get_text_content': '''
        Playwright Command - Get Text Content

        Finds the specified element using the selector specified by the action's selector 
        attribute and returns all text inside the element.
        
        ''',
        'get_page_title': '''
        Playwright Command - Get Page Title

        Returns the title of the current page.
        
        ''',
        'input_text': '''
        Playwright Command - Input Text

        Finds the specified element using the selector specified by the action's selector 
        attribute and inputs the text specified under the 'text' key of the dictionary
        under the action's data attribute.

        action.data['text']
        
        ''',
        'uncheck': '''
        Playwright Command - Uncheck

        Finds the specified radio checkbox element using the selector specified by
        the action's selector attribute and deselects it.
        
        ''',
        'wait_for_event': '''
        Playwright Command - Wait for Event

        Waits for the event type specified under the 'event' key of the dictionary
        under action's data attribute to execute.

        action.data['event']
        
        ''',
        'wait_for_function': '''
        Playwright Command - Wait for function

        Waits for the function specified under the 'function' key of the dictionary
        under action's data attribute to execute.

        action.data['function']

        ''',
        'wait_for_page_load_state': '''
        Playwright Command - Wait for Page Load State

        Waits for the page to load to the state specified under the 'state' key of
        the dictionary under the action's data attribute:

        action.data['state']
        
        ''',
        'wait_for_selector': '''
        Playwright Command - Wait for Selector

        Waits for the element found by the selector specified by the action's selector
        attribute to reach the state (active, visible, hidden, etc.) specified under
        the 'state' key of the dictionary under the action's data attribute:

        action.data['state']
        
        ''',
        'wait_for_timeout': '''
        Playwright Command - Wait for Timeout

        Waits for the timeout amount specified under the 'timeout' key of the dictionary
        under the action's data attribute.

        action.data['timeout']
        
        ''',
        'wait_for_url': '''
        Playwright Command - Wait for Url

        Waits for the url specified by the action's url attribute.

        ''',
        'switch_frame': '''
        Playwright Command - Switch Frame

        Switches to the frame specified. If the action's url attribute is specified, Playwright
        will switch to the frame specified by the url. Otherwise, Playwright will switch to the
        frame specified by the value under the 'frame' key of the dictionary under the action's 
        data attribute.
        
        ''',
        'get_frames': '''
        Playwright Command - Get Frames

        Returns all iFrames within the given page.
        
        ''',
        'switch_active_tab': '''
        Playwright Command - Switch Active Tab

        Switches the active tab.
        ''',
        'get_attribute': '''
        Playwright Command - Get Attribute

        Finds the element specified by the action's selector attribute and returns the value
        of the attribute specified by the 'attribute' key under the dictionary of the action's
        data attribute.

        action.data['attribute']
        
        ''',
        'go_back_page': '''
        Playwright Command - Go Back Page


        Returns to the previous page.
        
        ''',
        'go_forward_page': '''
        Playwright Command - Go Forward Page

        Goes to the next page.
        
        '''
    }