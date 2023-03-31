import json
from typing import Any, Dict, Union
from hedra.core.engines.types.task.result import TaskResult
from .base_processed_result import BaseProcessedResult


class TaskProcessedResult(BaseProcessedResult):

    def __init__(
        self, 
        stage: str, 
        result: TaskResult
    ) -> None:
        super(
            TaskProcessedResult,
            self
        ).__init__(
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

    def to_dict(self) -> Dict[str, Union[str, int, float]]:
        return {
            'name': self.name,
            'stage': self.stage,
            'shortname': self.shortname,
            'checks': [check.__name__ for check in self.checks],
            'error': str(self.error),
            'time': self.time,
            'type': self.type,
            'source': self.source,
            'data': self.data,
            **self.timings
        }
    
    def serialize(self) -> str:
        return json.dumps(
            self.to_dict()
        )