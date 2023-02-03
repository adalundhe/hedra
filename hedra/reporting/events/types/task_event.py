from typing import Any, Tuple, Dict
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.engines.types.task.result import TaskResult
from .base_result import BaseEvent


class TaskEvent(BaseEvent):

    def __init__(
        self, 
        stage: Any, 
        result: TaskResult
    ) -> None:
        super().__init__(
            stage,
            result
        )

        self.time = result.complete - result.start
        self.timings = {
            'total': self.time,
            'waiting': result.start - result.wait_start,
            'reading': result.complete - result.write_end
        }

        self.data = result.data