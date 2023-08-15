import binascii
import json
import uuid
from datetime import datetime
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.grpc.action import GRPCAction
from hedra.core.engines.types.grpc.protobuf_registry import protobuf_registry
from typing import Dict, Any, List


try:
    from cassandra.cqlengine import columns
    from cassandra.cqlengine.models import Model
    has_connector = True

except Exception:
    columns = object
    Model = None
    has_connector = False


class CassandraGRPCActionSchema:

    def __init__(self, table_name: str) -> None:
        self.action_columns = {
            'id': columns.UUID(primary_key=True, default=uuid.uuid4),
            'name': columns.Text(min_length=1, index=True),
            'method': columns.Text(min_length=1),
            'url': columns.Text(min_length=1),
            'headers': columns.Text(),
            'protobuf': columns.Text(),
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
            f'{table_name}_grpc',
            (Model, ), 
            self.action_columns
        )

        self.type = RequestTypes.GRPC

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
        action: GRPCAction
    ) -> Dict[str, Any]:
        


        encoded_protobuf = str(
            binascii.b2a_hex(
                action.data.SerializeToString()
            ), 
            encoding='raw_unicode_escape'
        )\
        
        encoded_message_length = hex(
            int(len(encoded_protobuf)/2)
        ).lstrip("0x").zfill(8)

        encoded_protobuf = f'00{encoded_message_length}{encoded_protobuf}'
        encoded_data = binascii.a2b_hex(encoded_protobuf)

        if protobuf_registry.get(action.name) is None:
            protobuf_registry[action.name] = action.data

        return {
            'id': uuid.UUID(action.action_id),
            'name': action.name,
            'url': action.url,
            'headers': json.dumps(action.headers),
            'protobuf': encoded_data,
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


        wire_msg = binascii.b2a_hex(
            action.get('protobuf')
        )

        message_length = wire_msg[4:10]
        msg = wire_msg[10:10+int(message_length, 16)*2]

        protobuf = binascii.a2b_hex(msg)

        return {
            'engine': RequestTypes.GRAPHQL,
            'name': action.get('name'),
            'url': action.get('url'),
            'method': action.get('method'),
            'headers': json.loads(action.get('headers')),
            'data': protobuf,
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
