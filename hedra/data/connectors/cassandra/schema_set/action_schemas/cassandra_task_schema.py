import json
import uuid
from datetime import datetime
from hedra.core.engines.types.task.task import Task
from hedra.core.engines.types.common.types import RequestTypes
from typing import Dict, Any, List


try:
    from cassandra.cqlengine import columns
    from cassandra.cqlengine.models import Model
    has_connector = True

except Exception:
    columns = object
    Model = None
    has_connector = False


class CassandraTaskSchema:

    def __init__(self, table_name: str) -> None:
        self.action_columns = {
            'id': columns.UUID(primary_key=True, default=uuid.uuid4),
            'name': columns.Text(min_length=1, index=True),
            'task_name': columns.Text(min_length=1),
            'env': columns.Text(),
            'user': columns.Text(),
            'tags': columns.List(
                columns.Map(
                    columns.Text(),
                    columns.Text()
                )
            ),
            'created_at': columns.DateTime(default=datetime.now)
        }

        self.actions_table = type(
            f'{table_name}_task',
            (Model, ), 
            self.action_columns
        )

        self.type = RequestTypes.TASK

        self._types_map: Dict[type, str] = {
            str: 'string',
            int: 'integer',
            float: 'float',
            bytes: 'bytes',
            bool: 'bool'
        }

        self._reverse_types_map: Dict[str, type] = {
            mapped_name: mapped_type for mapped_type, mapped_name in self._types_map.items()
        }
    
    def to_schema_record(
        self,
        task: Task
    ) -> Dict[str, Any]:
    

        return {
            'id': uuid.UUID(task.action_id),
            'name': task.name,
            'task_name': task.execute.__name__,
            'env': task.event,
            'user': task.metadata.user,
            'tags': [
                {
                    'name': tag.get('name'),
                    'value': str(tag.get('value')),
                    'datatype': self._types_map.get(type(
                        tag.get('value')
                    ))
                } for tag in task.metadata.tags
            ]
        }
    
    def from_schema_record(
        self,
        action: Dict[str, Any]
    ) -> Dict[str, Any]:
        
        action_tags: List[Dict[str, Any]] = action.get('tags', [])


        return {
            'engine': RequestTypes.GRAPHQL,
            'name': action.get('name'),
            'task_name': action.get('task_name'),
            'env': action.get('env'),
            'user': action.get('user'),
            'tags': [
                {
                    'name': tag.get('name'),
                    'value': self._reverse_types_map.get(
                        tag.get('datatype')
                    )(
                        tag.get('value')
                    )
                } for tag in action_tags
            ]
        }