import json
import uuid
from datetime import datetime
from hedra.core.engines.types.playwright.command import PlaywrightCommand
from hedra.core.engines.types.common.types import RequestTypes
from typing import (
    Dict, 
    Any, 
    List, 
    Type,
    Union,
    Callable
)


try:
    from cassandra.cqlengine import columns
    from cassandra.cqlengine.models import Model
    has_connector = True

except Exception:
    columns = object
    Model = None
    has_connector = False


class CassandraPlaywrightActionSchema:

    def __init__(self, table_name: str) -> None:
        self.action_columns = {
            'id': columns.UUID(primary_key=True, default=uuid.uuid4),
            'name': columns.Text(min_length=1, index=True),
            'command': columns.Text(min_length=1),
            'selector': columns.Text(min_length=1),
            'attribute': columns.Text(min_length=1),
            'x_coordinate': columns.Text(min_length=1),
            'y_coordinate': columns.Text(min_length=1),
            'frame': columns.Integer(default=0),
            'location': columns.Text(),
            'headers': columns.Text(),
            'key': columns.Text(),
            'text': columns.Text(),
            'expression': columns.Text(),
            'args': columns.List(
                columns.Map(
                    columns.Text(),
                    columns.Text()
                )
            ),
            'filepath': columns.Text(),
            'file': columns.Text(),
            'path': columns.Text(),
            'option': columns.Map(
                columns.Text(),
                columns.Text()
            ),
            'by_label': columns.Boolean(default=False),
            'by_value': columns.Boolean(default=False),
            'event': columns.Text(),
            'is_checked': columns.Boolean(default=False),
            'timeout': columns.Float(),
            'extra': columns.Text(),
            'switch_by': columns.Text(),
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
            f'{table_name}_playwright',
            (Model, ), 
            self.action_columns
        )

        self.type = RequestTypes.PLAYWRIGHT

        self._types_map: Dict[type, str] = {
            str: 'string',
            int: 'integer',
            float: 'float',
            bool: 'boolean',
            bytes: 'bytes',
            dict: 'dictionary',
            list: 'list'
        }

        self._serialize_map: Dict[
            str, 
            Callable[
                [Type[
                    Union[
                        str, 
                        bytes, 
                        int, 
                        float, 
                        list, 
                        dict
                    ]
                ]],
                str
            ]
        ] = {
            str: str,
            bytes: str,
            int: str,
            float: str,
            bool: str,
            dict: json.dumps,
            list: json.dumps
        }
    

        self._deserialize_map: Dict[
            str, 
            Callable[
                [str],
                Type[
                    Union[
                        str, 
                        bytes, 
                        int, 
                        float, 
                        list, 
                        dict
                    ]
                ]
            ]
        ] = {
            'string': str,
            'integer': int,
            'float': float,
            'bool': lambda boolean: boolean == "True",
            'bytes': bytes,
            'dictionary': json.loads,
            'list': json.loads
        }
    
    def to_schema_record(
        self,
        action: PlaywrightCommand
    ) -> Dict[str, Any]:
        
        action_option: Union[Dict[str, str], None] = None
        if action.input.option:
            action_option = {
                    'data': self._serialize_map.get(type(
                        action.input.option
                    ))(action.input.option),
                    'datatype': self._types_map.get(type(
                        action.input.option
                    ))
            }


        return {
            'id': uuid.UUID(action.action_id),
            'name': action.name,
            'url': action.url,
            'selector': action.page.selector,
            'attribute': action.page.attribute,
            'x_coordinate': action.page.x_coordinate,
            'y_coordinate': action.page.y_coordinate,
            'frame': action.page.frame,
            'location': action.url.location,
            'headers': json.dumps(action.url.headers),
            'key': action.input.key,
            'text': action.input.text,
            'expression': action.input.expression,
            'args': [
                {
                    'data': self._serialize_map.get(type(
                        arg
                    ))(arg),
                    'datatype': self._types_map.get(type(
                        arg
                    ))
                } for arg in action.input.args
            ],
            'filepath': action.input.filepath,
            'file': action.input.file,
            'path': action.input.path,
            'option': action_option,
            'by_label': action.input.by_label,
            'by_value': action.input.by_value,
            'event': action.options.event,
            'is_checked': action.options.is_checked,
            'timeout': action.options.timeout,
            'extra': json.dumps(action.options.extra),
            'switch_by': action.options.switch_by,
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
        action_args: List[Dict[str, Any]] = action.get('args', [])
        action_option: Dict[str, Any] = action.get('option')

        option: Union[
            str, 
            bytes, 
            int, 
            float, 
            bool, 
            list, 
            dict, 
            None
        ] = None

        if action_option:
            option = self._deserialize_map.get(
                action_option.get('datatype')
            )(
                action_option.get('data')
            )

        return {
            'id': uuid.UUID(action.get('action_id')),
            'name': action.get('name'),
            'url': action.get('url'),
            'selector': action.get('selector'),
            'attribute': action.get('attribute'),
            'x_coordinate': action.get('x_coordinate'),
            'y_coordinate': action.get('y_coordinate'),
            'frame': action.get('frame'),
            'location': action.get('location'),
            'headers': json.loads(
                action.get('headers', {})    
            ),
            'key': action.get('key'),
            'text': action.get('text'),
            'expression': action.get('expression'),
            'args': [
                self._deserialize_map.get(
                    arg_config.get('datatype')
                )(
                    arg_config.get('data')
                ) for arg_config in action_args
            ],
            'filepath': action.get('filepath'),
            'file': action.get('file'),
            'path': action.get('path'),
            'option': option,
            'by_label': action.get('by_label'),
            'by_value': action.get('by_Value'),
            'event': action.get('event'),
            'is_checked': action.get('is_checked'),
            'timeout': action.get('timeout'),
            'extra': json.loads(
                action.get('extra', {})
            ),
            'switch_by': action.get('switch_by'),
            'user': action.get('user'),
            'tags': action_tags
        }
