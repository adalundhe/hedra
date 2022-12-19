import functools
from .hook_type import HookType
from hedra.core.graphs.hooks.registry.registrar import registrar


@registrar(HookType.TEARDOWN)
def teardown(metadata={}):
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

        @functools.wraps(func)
        async def decorator(*args, **kwargs):
            return await func(*args, **kwargs)

        return decorator
    return wrapper