import uuid


class CassandraHelper:

    def __init__(self, keyspace_config=None):
        if keyspace_config is None:
            keyspace_config = {}

        self.keyspace_config = keyspace_config
        self.session_id = uuid.uuid4()
        self.field_types = {
            str: 'TEXT',
            int: 'BIGINT',
            float: 'DOUBLE',
            dict: 'BLOB',
            bool: 'BOOLEAN'
        }

        self.default_table_fields = [
            {
                'name': 'id',
                'type': 'UUID'
            },
            {
                'name': 'session_id',
                'type': 'UUID'
            },
            {
                'name': 'format',
                'type': 'TEXT'
            },
            {
                'name': 'timestamp',
                'type': 'TIMESTAMP'
            }
        ]

        self.default_tags_table_fields = [
            {
                'name': 'id',
                'type': 'UUID'
            },
            {
                'name': 'session_id',
                'type': 'UUID'
            },
            {
                'name': 'event_or_metric_id',
                'type': 'UUID'
            },
            {
                'name': 'format',
                'type': 'TEXT'
            },
            {
                'name': 'timestamp',
                'type': 'TIMESTAMP'
            },
            {
                'name': 'tag_name',
                'type': 'TEXT'
            },
            {
                'name': 'tag_value',
                'type': 'TEXT'
            }
        ]

    def set_keyspace(self) -> None:

        if self.keyspace_config.get('name') is None:
            self.keyspace_config['name'] = '"session_{session_uuid}"'.format(
                session_uuid=uuid.uuid4()
            ).replace('-', '_')

        keyspace_options = self.keyspace_config.get('keyspace_options')

        if keyspace_options is None:
            keyspace_options = {}

        if keyspace_options.get('replication_options') is None:
            keyspace_options['replication_options'] = {
                "class": "SimpleStrategy", 
                "replication_factor": 3
            }

        self.keyspace_config['keyspace_options'] = keyspace_options


    def as_table(self, table_name=None, data=None) -> dict:

        table_fields = []
        for field_name, field_type in data.fields.items():
            table_fields.append({
                'name': field_name,
                'type': self.field_types.get(field_type)
            })

        table_fields.extend(self.default_table_fields)

        return {
            'keyspace': self.keyspace_config.get('name'),
            'table': table_name,
            'fields': table_fields,
            'table_options': {
                'primary_key_fields': ['id', 'session_id'],
                'clustering_order': {
                    'clustering_order_field': 'session_id'
                }
            }
        }

    def tags_as_table(self, table_name=None) -> dict:
        return {
            'keyspace': self.keyspace_config.get('name'),
            'table': table_name,
            'fields': self.default_tags_table_fields,
            'table_options': {
                'primary_key_fields': ['id', 'session_id'],
                'clustering_order': {
                    'clustering_order_field': 'session_id'
                }
            }
        }

    def to_record(self, table=None, data=None, timestamp=None) -> dict:
        record_id = uuid.uuid4()
        fields = self._as_field_names(self.default_table_fields)

        values = []
        
        data_dict = data.to_dict()
        for field in data.fields:
            fields.append(field)
            values.append(data_dict.get(field))

        tag_records = []
        for tag in data.tags:
            tag_records.append({
                'type': 'insert',
                'keyspace': self.keyspace_config.get('name'),
                'table': table,
                'fields': self._as_field_names(self.default_tags_table_fields),
                'values': [
                    uuid.uuid4(),
                    self.session_id,
                    record_id,
                    tag.format,
                    timestamp,
                    tag.name, 
                    tag.value
                ]
            })

        return {
            'table': {
                'type': 'insert',
                'keyspace': self.keyspace_config.get('name'),
                'table': table,
                'fields': fields,
                'values': [
                    record_id, 
                    self.session_id, 
                    data.format, 
                    timestamp,
                    *values
                ]
            },
            'tags_table': tag_records
        }

    def to_table_query(self, table=None, data=None) -> dict:
        return {
            'table': {
                'keyspace': self.keyspace_config.get('name'),
                'table': table,
                'fields': [
                    {
                        'name': field
                    } for field in self._as_field_names(
                        data.fields, 
                        self.default_table_fields
                    )
                ]
            },
            'tags_table': {
                'keyspace': self.keyspace_config.get('name'),
                'table': table,
                'fields': [
                    {
                        'name': field
                    } for field in self._as_field_names(
                        data.fields, 
                        self.default_tags_table_fields
                    )
                ]
            }
        }

    def _as_field_names(self, *fields_configs) -> list:
        fields =[]
        for field_config in fields_configs:
            fields.extend([ field.get('name') for field in field_config ])

        return fields

