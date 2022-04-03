class TableStatement:

    def __init__(self, query):
        self.table_name = query.get('table')
        self.keyspace = query.get('keyspace')
        self.fields = query.get('fields')
        self.table_options = query.get('table_options')

    async def create_fields_strings(self) -> str:

        field_strings = []

        for field in self.fields:

            field_name = field.get('name')
            field_type = field.get('type')

            field_string = f'{field_name} {field_type}'
            
            if field.get('primary_key'):
                field_string = f'{field_string} PRIMARY KEY'

            field_strings.append(field_string)

        return ', '.join(field_strings)

    async def assemble_create_statement(self) -> str:

        fields_strings = await self.create_fields_strings()

        create_table_statement = f'CREATE TABLE {self.keyspace}.{self.table_name}( {fields_strings} '

        if self.table_options:

            if self.table_options.get('primary_key_fields'):
                primary_key_fields = ', '.join(self.table_options.get('primary_key_fields'))

                create_table_statement = f'{create_table_statement}, PRIMARY KEY ({primary_key_fields})'

            if self.table_options.get('clustering_order'):

                clustering_order_config = self.table_options.get('clustering_order')
                ordering_field_name = clustering_order_config.get('clustering_order_field')
                ordering_direction = clustering_order_config.get('ordering_direction', 'DESC')

                create_table_statement = f'{create_table_statement}) WITH CLUSTERING ORDER BY ({ordering_field_name} {ordering_direction}'

        return f'{create_table_statement});'

    async def assemble_delete_statement(self) -> str:
        return f'DROP TABLE {self.table_name};'