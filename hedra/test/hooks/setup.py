import functools

def setup(name, group=None, metadata={}):
    '''
    Setup Hook

    The @setup(<name>, ...) decorator is an optional means of specifying that
    a method of a class inherting ActionSet should executor prior to testing as a means
    of setting up/initializing test state. You may specify as many setup hooks
    as needed, however all methods wrapped in the decorator must be asynchronous
    and return valid results.

    You may pass the following to a setup hook:

    - name (required:string - positional)
    - group (optional:string - keyword)
    - metadata (optional:dict - keyword - may include env, user, type, tags [list], and url)
    
    '''
    def wrapper(func):
 
        func.action_name = name
        func.is_setup = True
        func.is_teardown = False
        func.is_action = True
        func.weight = 0
        func.order = 0
        func.group = group
        func.metadata = {
            'group': group
        }
        func.timeout = None
        func.wait_interval = None
        func.env = metadata.get('env')
        func.user = metadata.get('user')
        func.type = metadata.get('type')
        func.tags = metadata.get('tags')
        func.url = metadata.get('url')
        func.checks = None

        @functools.wraps(func)
        async def decorator(*args, **kwargs):
            return await func(*args, **kwargs)

        return decorator
    return wrapper