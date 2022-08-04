from typing import Any, Coroutine

from hedra.core.pipelines.hooks.types.types import HookType
from .hook import Hook


class Internal:
    is_internal = True

    def __init__(self, internal_call: Coroutine) -> None:
        self.call = internal_call

    def __call__(self) -> Hook:
        return Hook(
            self.call.__qualname__,
            self.call.__name__,
            self.call,
            HookType.INTERNAL
        )