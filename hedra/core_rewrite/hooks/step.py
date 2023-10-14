from functools import wraps
from .hook import Hook

from typing import (
    Callable,
    Awaitable,
    Any
)


def step(*args: str):

    def wrapper(func: Callable[..., Awaitable[Any]]):
        
        return Hook(
            func,
            args
        )
    
    return wrapper