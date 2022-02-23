from .utils import (
    assemble_fields_string,
    format_values
)


class QueryStatement:

    def __init__(self, query):
        self.keyspace = query.get('keyspace')
        self.table_name = query.get('table')
        self.distinct = query.get('distinct')
        self.fields = query.get('fields')
        self.values = query.get('values')
        self.type = query.get('type', 'select')

    async def assemble_query_fields(self) -> str:
        field_statements = []

        for field in self.fields:
            field_name=field.get('name')
            field_string = f'{field_name}'

            if field.get('alias'):
                field_alias = field.get('alias')
                field_string = f'{field_string} AS {field_alias}'

            field_statements.append(field_string)

        return ', '.join(field_statements)

    async def assemble_field_value_pairs(self, fields, values):
        field_value_pairs = []
        for idx, field in enumerate(fields):
            field_value_pair = f'{field}={values[idx]}'

            field_value_pairs.append(field_value_pair)
        
        return ', '.join(field_value_pairs)

    async def assemble_select_statement(self, is_insert=False) -> str:

        query_statement_string = "SELECT"

        if self.distinct:
            query_statement_string = "SELECT DISTINCT"

        query_fields = await self.assemble_query_fields()

        query_statement_string = f"{query_statement_string} {query_fields} FROM"

        if self.keyspace:
            query_statement_string = f"{query_statement_string} {self.keyspace}.{self.table_name}"

        else:
            query_statement_string = f"{query_statement_string} {self.table_name}"

        return query_statement_string

    async def assemble_insert_statement(self) -> str:
        if self.keyspace:
            insert_string =  f"INSERT INTO {self.keyspace}.{self.table_name}"
        
        else:
            insert_string = f"INSERT INTO {self.table_name}"

        
        fields = ', '.join(self.fields)
        values = ', '.join(format_values(values=self.values))

        return f"{insert_string} ({fields}) VALUES ({values})"

    async def assemble_update_statement(self) -> str:
        if self.keyspace:
            update_string = f"UPDATE {self.keyspace}.{self.table_name}"
        
        else:
            update_string = f"UPDATE {self.table_name}"

        field_value_pairs = await self.assemble_field_value_pairs(
            self.fields,
            format_values(values=self.values)
        )

        return f'{update_string} SET {field_value_pairs}'

    async def assemble_delete_statement(self) -> str:

        fields = ', '.join(self.fields)

        delete_string = f"DELETE {fields}"

        if self.keyspace:
            delete_string = f'{delete_string} FROM {self.keyspace}.{self.table_name}'
        
        else:
            delete_string = f'{delete_string} FROM {self.table_name}'
        