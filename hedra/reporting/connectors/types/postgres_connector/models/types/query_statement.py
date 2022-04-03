from .utils import (
    assemble_fields_string,
    format_values
)


class QueryStatement:

    def __init__(self, query, field_types=None):
        self.table_name = query.get('table')
        self.fields = query.get('fields')
        self.values = query.get('values')
        self.type = query.get('type')
        self._field_types = field_types

    def assemble_query_string(self, is_insert=False):
        return "SELECT {fields} FROM {table}".format(
            fields=assemble_fields_string(
                table_name=self.table_name,
                fields=self.fields
            ),
            table=self.table_name
        )

    def assemble_insert_statement(self):
        return "INSERT INTO {table} ({fields}) VALUES ({values})".format(
            table=self.table_name,
            fields=', '.join(self.fields),
            values=', '.join(
                format_values(
                    self.fields,
                    self.values, 
                    field_types=self._field_types
                )
            )
        )

    def assemble_update_statement(self):
        formatted_values = format_values(
            self.fields,
            self.values, 
            field_types=self._field_types
        )

        column_field_strings = ', '.join([ 
            '{field} = {value}'.format(
                field=field,
                value=value
            ) for field, value in zip(self.fields, formatted_values)
         ])

        return "UPDATE {table} SET {column_field_strings}".format(
            table=self.table_name,
            column_field_strings=column_field_strings
        )

    def assemble_delete_statement(self):
        return "DELETE FROM {table}".format(
            table=self.table_name
        )

    