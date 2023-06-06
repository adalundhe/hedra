import json
import uuid
from datetime import datetime
from hedra.core.engines.types.http.action import HTTPAction
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


class CassandraHTTPActionSchema:

    def __init__(self, table_name: str) -> None:
        self.action_columns = {
            'id': columns.UUID(primary_key=True, default=uuid.uuid4),
            'name': columns.Text(min_length=1, index=True),
            'method': columns.Text(min_length=1),
            'url': columns.Text(min_length=1),
            'headers': columns.Text(),
            'data': columns.Text(),
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
            f'{table_name}_http',
            (Model, ), 
            self.action_columns
        )

        self.type = RequestTypes.HTTP

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
        action: HTTPAction
    ) -> Dict[str, Any]:
        
        action_data = action.data

        if isinstance(action_data, (bytes,)):
            action_data = {
                'data': str(action_data),
                'datatype': 'bytes'
            }

        elif isinstance(action_data, (int, )):
            action_data = {
                'data': str(action_data),
                'datatype': 'integer'
            }
        
        elif isinstance(action_data, (float, )):
            action_data = {
                'data': str(action_data),
                'datatype': 'float'
            }

        else: 
            action_data = {
                'data': action_data,
                'datatype': 'string'
            }

        return {
            'id': uuid.UUID(action.action_id),
            'name': action.name,
            'url': action.url,
            'headers': json.dumps(action.headers),
            'data': json.dumps(action_data),
            'user': action.metadata.user,
            'tags': [
                {
                    'name': tag.get('name'),
                    'value': str(tag.get('value')),
                    'datatype': self._types_map.get(type(
                        tag.get('value')
                    ))
                } for tag in action.metadata.tags
            ]
        }
    
    def from_schema_record(
        self,
        action: Dict[str, Any]
    ) -> Dict[str, Any]:
        
        action_tags: List[Dict[str, Any]] = action.get('tags', [])
        action_data: Dict[str, str] = json.load(action.get('data'))

        data = action_data.get('data')
        datatype = action_data.get('datatype')

        if datatype == 'bytes':
            data = data.encode()

        elif datatype == 'integer':
            data = int(data)
        
        elif datatype == 'float':
            data = float(data)

        return {
            'engine': RequestTypes.GRAPHQL,
            'name': action.get('name'),
            'url': action.get('url'),
            'method': action.get('method'),
            'headers': json.loads(action.get('headers')),
            'data': data,
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
