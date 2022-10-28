
from typing import Generic, TypeVar
from hedra.core.engines.types.common.base_result import BaseResult
from .action import Action

R = TypeVar('R')

class Result(BaseResult, Generic[R]):

    __slots__ = (
        'times',
        "type"
    )

    def __init__(self, action: Action, error: Exception = None) -> None:
        super().__init__(
            action.name, 
            action.source,
            action.metadata.user,
            action.metadata.tags,
            action.plugin_type,
            action.hooks.checks,
            error
        )

        self.times = {}

    def as_timings(self):
        return {
            'total': self.times['complete'] - self.times['start']
        }