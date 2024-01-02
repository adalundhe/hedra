from hedra.core_rewrite.engines.client.client_types.common import Timeouts
from functools import wraps
from .hook import Hook

from typing import (
    Callable,
    Awaitable,
    Optional,
    Any
)


def step(
    *args: str, 
    timeouts: Optional[Timeouts]=None
):

    def wrapper(func: Callable[..., Awaitable[Any]]):
        
        return Hook(
            func,
            args
        )
    
    return wrapper