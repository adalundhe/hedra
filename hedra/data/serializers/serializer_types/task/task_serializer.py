from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.task.task import Task
from hedra.core.engines.types.task.runner import MercuryTaskRunner
from hedra.core.engines.types.task.result import TaskResult
from hedra.core.engines.types.common.types import RequestTypes
from hedra.data.serializers.serializer_types.common.base_serializer import BaseSerializer
from typing import List, Dict, Union, Any


class TaskSerializer(BaseSerializer):

    def __init__(self) -> None:
        super().__init__()

    def action_to_serializable(
        self,
        task: Task
    ) -> Dict[str, Union[str, List[str]]]:
        
        serialized_action = super().action_to_serializable(task)
        return {
            **serialized_action,
            'type': RequestTypes.TASK,
            'source': task.source,
            'task_action': task.execute.__name__,
            'user': task.metadata.user,
            'tags': task.metadata.tags
        }
    
    def deserialize_task(
        self,
        task: Dict[str, Any]
    ) -> Task:
        
        deserialized_task = Task(
            task.get('name'),
            None,
            source=task.get('source'),
            user=task.get('user'),
            tags=task.get('tags', [])
        )

        return deserialized_task
    
    def deserialize_client_config(self, client_config: Dict[str, Any]) -> MercuryTaskRunner:
        return MercuryTaskRunner(
            concurrency=client_config.get('concurrency'),
            timeouts=Timeouts(
                **client_config.get('timeouts', {})
            )
        )
    
    def result_to_serializable(
        self,
        result: TaskResult
    ) -> Dict[str, Any]:
    
        serialized_result = super().result_to_serializable(result)
        return {
            **serialized_result,
            'data': result.data,
        }
    
    def deserialize_result(
        self,
        result: Dict[str, Any]
    ) -> TaskResult:
        
        task_action = Task(
            result.get('name'),
            None,
            source=result.get('source'),
            user=result.get('user'),
            tags=result.get('tags')
        )

        task_result = TaskResult(
            task_action,
            error=result.get('error')
        )

        task_result.data = result.get('data')
     
  
        task_result.checks = result.get('checks')
        task_result.wait_start = result.get('wait_start')
        task_result.start = result.get('start')
        task_result.connect_end = result.get('connect_end')
        task_result.write_end = result.get('write_end')
        task_result.complete = result.get('complete')

        return task_result
