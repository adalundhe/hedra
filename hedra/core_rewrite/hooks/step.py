from typing import Any, Awaitable, Callable, Optional

from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .hook import Hook


def step(*args: str, timeouts: Optional[Timeouts] = None):
    def wrapper(func: Callable[..., Awaitable[Any]]):
        return Hook(func, args)

    return wrapper
