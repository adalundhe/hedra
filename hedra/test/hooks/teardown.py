import functools


def teardown(name, group=None, metadata={}):
    '''
    Teardown Hook

    The @teardown(<name>, ...) decorator is an optional means of specifying that
    a method of a class inherting ActionSet should executor after testing as a means
    of cleaning up/closing any session or state created during tests. You may specify as 
    many teardown hooks as needed, however all methods wrapped in the decorator must be 
    asynchronous and return valid results.

    You may pass the following to a teardown hook:

    - name (required:string - positional)
    - group (optional:string - keyword)
    - metadata (optional:dict - keyword - may include env, user, type, tags [list], and url)
    
    '''
    def wrapper(func):

        func.action_name = name
        func.is_action = True
        func.is_before = False
        func.is_after = False
        func.is_setup = False
        func.is_teardown = True
        func.weight = 0
        func.order = float('inf')
        func.group = group
        func.timeout = None
        func.wait_interval = None
        func.metadata = {
            'group': group
        }
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