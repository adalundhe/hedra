import asyncio
import contextvars
import functools


async def awaitable(func, *args, **kwargs):
    """
    NOTE: Due to a Python 3.9 bug with OSX
          we can't use asyncio.to_thread(), so we
          have to re-implement it here.
    """
    loop = asyncio.get_running_loop()
    ctx = contextvars.copy_context()
    func_call = functools.partial(ctx.run, func, *args, **kwargs)
    return await loop.run_in_executor(None, func_call)




