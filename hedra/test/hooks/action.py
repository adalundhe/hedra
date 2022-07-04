import functools

def action(name, group=None, weight=1, order=1, timeout=None, wait_interval=None, metadata={}, checks=None):
    '''
    Action Hook

    The @action(<name>, ...) decorator is required for any methods of a class
    inheriting the ActionSet class to execute as actions. All functions wrapped in the
    decorator must be asynchronous and return valid results.

    You may pass the following to a setup hook:

    - name (required:string - positional)
    - group (optional:string - keyword)
    - weight (optional:decimal - keyword)
    - order (optional:integer - keyword)
    - timeout (optional:integer - keyword - use in place of request_timeout)
    - wait_interval (optional:integer - keyword - used in place of batch interval for Sequence based personas)
    - metadata (optional:dict - keyword - may include env, user, type, tags [list:string], and url)
    - success_condition (optional:function - keyword)

    Note that the success condition function, if provided, will be executed when results are
    assessed. The success condition function should accept a single positional argument 
    representing the response returned by the action (for example, the HTTP response object
    returned by a request made using AioHTTP and return a true/false value based on that argument.

    For example:

    @action('my_test_action', success_condition=lambda response: response.status > 200 and response.status < 400)
    def my_test_action(self):
        with self.session.get('https://www.google.com') as response:
            return await response

    '''
    def wrapper(func):
        func.name = name
        func.is_action = True
        func.is_before = False
        func.is_after = False
        func.is_setup = False
        func.is_teardown = False
        func.weight = weight
        func.order = order
        func.group = group
        func.metadata = {
            'group': group
        }
        func.timeout = timeout
        func.wait_interval = wait_interval
        func.env = metadata.get('env')
        func.user = metadata.get('user')
        func.type = metadata.get('type')
        func.tags = metadata.get('tags')
        func.url = metadata.get('url')
        func.checks = checks
        func.context = None

        @functools.wraps(func)
        def decorator(*args, **kwargs):
            return func(*args, **kwargs)
                
        return decorator  
    return wrapper