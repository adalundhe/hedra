import contextvars
import functools


async def wrap(func, *args, **kwargs):
    ctx = contextvars.copy_context()
    return functools.partial(ctx.run, func, *args, **kwargs)