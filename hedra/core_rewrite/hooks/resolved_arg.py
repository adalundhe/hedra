from typing import TypeVar, Generic
from .call_arg import CallArg
from .resolved_arg_type import ResolvedArgType


T = TypeVar('T')


class ResolvedArg(Generic[T]):

    def __init__(
        self,
        arg_type: ResolvedArgType,
        call_arg: CallArg,
        value: T
    ) -> None:
        self.arg = call_arg
        self.arg_type = arg_type
        self.value = value