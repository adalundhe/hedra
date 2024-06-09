from typing import TypeVar, Generic
from .call_arg import CallArg
from .resolved_arg_type import ResolvedArgType
from .resolved_auth import ResolvedAuth
from .resolved_data import ResolvedData
from .resolved_headers import ResolvedHeaders
from .resolved_method import ResolvedMethod
from .resolved_params import ResolvedParams
from .resolved_query import ResolvedQuery
from .resolved_url import ResolvedURL


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

    @property
    def data(self) -> (
        ResolvedAuth | 
        ResolvedData | 
        ResolvedHeaders | 
        ResolvedMethod |
        ResolvedParams |
        ResolvedQuery |
        ResolvedURL
    ):
        return self.value