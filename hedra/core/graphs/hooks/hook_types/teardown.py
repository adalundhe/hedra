import functools
from .hook_type import HookType
from hedra.core.graphs.hooks.registry.registrar import registrar


@registrar(HookType.TEARDOWN)
def teardown(key: str=None, metadata={}):

    def wrapper(func):

        @functools.wraps(func)
        async def decorator(*args, **kwargs):
            return await func(*args, **kwargs)

        return decorator
    return wrapper