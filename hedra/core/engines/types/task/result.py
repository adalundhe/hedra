from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.engines.types.common.types import RequestTypes
from .task import Task


class TaskResult(BaseResult):

    __slots__ = (
        'action_id',
        'wait_start',
        'start',
        'complete',
        'data',
        'error'
    )

    def __init__(self, task: Task, error: Exception=None):
        super(
            TaskResult,
            self
        ).__init__(
            task.action_id,
            task.name,
            task.source,
            task.metadata.user,
            task.metadata.tags,
            RequestTypes.TASK,
            task.hooks.checks,
            error
        )

        self.wait_start = 0
        self.start = 0
        self.complete = 0

        self.data = None
        self.error = error