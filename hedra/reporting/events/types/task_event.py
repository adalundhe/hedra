from hedra.core.engines.types.task.result import TaskResult
from .base_event import BaseEvent


class TaskEvent(BaseEvent):

    def __init__(self, result: TaskResult) -> None:
        super().__init__(result)

        self.time = result.complete - result.start
        self.timings = {
            'total': self.time,
            'waiting': result.start - result.wait_start,
            'reading': result.complete - result.write_end
        }

        self.data = result.data