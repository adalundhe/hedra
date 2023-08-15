import asyncio
from typing import List


def _release_waiter(waiter: asyncio.Future, *args):
    if not waiter.done():
        waiter.set_result(None)


async def wait(
    fs: List[asyncio.Future], 
    timeout: float=None
):
    loop = asyncio.get_event_loop()
    """Internal helper for wait().
    The fs argument must be a collection of Futures.
    """
    waiter = loop.create_future()
    timeout_handle = None
    if timeout is not None:
        timeout_handle = loop.call_later(timeout, _release_waiter, waiter)
    counter = len(fs)

    def _on_completion(f: asyncio.Future):
        nonlocal counter
        counter -= 1
        if (counter <= 0 and (not f.cancelled() and
                                                f.exception() is not None)):
            if timeout_handle is not None:
                timeout_handle.cancel()
            if not waiter.done():
                waiter.set_result(None)

    for f in fs:
        f.add_done_callback(_on_completion)

    try:
        await waiter
    finally:
        if timeout_handle is not None:
            timeout_handle.cancel()
        for f in fs:
            f.remove_done_callback(_on_completion)

    done, pending = set(), set()
    for f in fs:
        if f.done():
            done.add(f)
        else:
            pending.add(f)

    return done, pending

