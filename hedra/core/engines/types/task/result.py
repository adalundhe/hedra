from typing import Dict, Union
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
            error
        )

        self.wait_start = 0
        self.start = 0
        self.complete = 0

        self.data = None
        self.error = error

    def to_dict(self):
        base_dict = super().to_dict()
        return {
            'data': self.data,
            **base_dict
        }

    @classmethod
    def from_dict(cls, results_dict: Dict[str, Union[int, float, str,]]):

        task_action = Task(
            results_dict.get('name'),
            None,
            source=results_dict.get('source'),
            user=results_dict.get('user'),
            tags=results_dict.get('tags')
        )

        task_result = TaskResult(
            task_action,
            error=results_dict.get('error')
        )

        task_result.data = results_dict.get('data')
     
  
        task_result.checks = results_dict.get('checks')
        task_result.wait_start = results_dict.get('wait_start')
        task_result.start = results_dict.get('start')
        task_result.connect_end = results_dict.get('connect_end')
        task_result.write_end = results_dict.get('write_end')
        task_result.complete = results_dict.get('complete')

        return task_result