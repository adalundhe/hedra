import functools
from .hook_type import HookType
from hedra.core.graphs.hooks.registry.registrar import registrar


@registrar(HookType.SETUP)
def setup(metadata={}):
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
 
        @functools.wraps(func)
        async def decorator(*args, **kwargs):
            return await func(*args, **kwargs)

        return decorator
    return wrapper