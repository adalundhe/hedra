
class SQLHelper:

    def __init__(self, table_name=None, tags_table_name=None, model=None, optional_fields=None):
        self.table_name = table_name
        self.tags_table_name = tags_table_name
        self.model = model
      
        self.table_fields = []
        self.tags_table_fields = []
        self.optional_fields = []

        if optional_fields:
            self.optional_fields = optional_fields

    def create_table_configs(self, table_options=None):

        for field_name, field_type in self.model.fields.items():
            self.table_fields.append({
                'name': field_name,
                'type': field_type
            })

    
        return {
            'model_table': {
                'table': self.table_name,
                'fields': [
                    *self.table_fields,
                    *self.optional_fields
                ],
                'table_options': table_options
            },
            'tags_table': {
                'table': self.tags_table_name,
                'fields': [
                    {
                        'name': 'tag_name',
                        'type': str
                    }, 
                    {
                        'name': 'tag_value',
                        'type': str
                    }, 
                    *self.optional_fields
                ],
                'table_options': table_options
            }
        }

    def create_insert_statements(self, data, optional_field_values=None, options=None):

        table_field_names = self._get_table_field_names()
        optional_field_names = self._get_optional_field_names()

        values = []
        data_dict = data.to_dict()
        
        for field in table_field_names:
            values.append(data_dict.get(field))

        if optional_field_values:
            for field_name in optional_field_names:
                field_value = optional_field_values.get(field_name)
                values.append(field_value)

        return {
            'model': {
                'type': 'insert',
                'table': self.table_name,
                'fields': [
                    *table_field_names,
                    *optional_field_names
                ],
                'values': values,
                'options': options            
            },
            'tags': self._tags_to_record(
                data,
                optional_field_values=optional_field_values,
                options=options
            )
        }

    def create_query(self, fields=None, options=None):
        
        if fields is None:
            fields = '*'

        else:
            fields = ', '.join(fields)

        return {
            'type': 'select',
            'table': self.table_name,
            'fields': fields,
            'options':options
        }

    def _tags_to_record(self, data, optional_field_values=None, options=None):
        tag_records = []
        optional_field_names = self._get_optional_field_names()

        for tag in data.tags:
            values = [ tag.name, tag.value ]

            if optional_field_values:

                for field_name in optional_field_names:
                    field_value = kwargs.get(field_name)
                    values.append(field_value) 

                tag_records.append({
                    'type': 'insert',
                    'table': self.tags_table_name,
                    'fields': [
                        'tag_name',
                        'tag_value',
                        *optional_field_names
                    ],
                    'values': values,
                    'options': options
                })

        return tag_records

    def _get_table_field_names(self):
        return [table_field.get('name') for table_field in self.table_fields]

    def _get_optional_field_names(self):
        return [optional_field.get('name') for optional_field in self.optional_fields]

