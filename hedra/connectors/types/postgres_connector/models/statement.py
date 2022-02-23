from .types import (
    TableStatement,
    QueryStatement,
    OptionsStatment
)


class Statement:

    def __init__(self, query, field_types=None):
        self.table_statement = TableStatement(query)
        self.query_statment = QueryStatement(query, field_types=field_types)
        self.options_statement = OptionsStatment(
            table_name=query.get('table'),
            options=query.get('options', [])
        )
        self.has_return = False
        self.is_transaction = False

    def assemble_query_statement(self) -> str:
        
        if self.table_statement.type == 'alter_table':
            return '{statement}'.format(
                statement=self.table_statement.assemble_alter_table_statement()
            )

        if self.query_statment.type == 'insert':
            query = self.query_statment.assemble_insert_statement()
            self.is_transaction = True

        elif self.query_statment.type == 'update':
            query = self.query_statment.assemble_update_statement()
            self.is_transaction = True

        elif self.query_statment.type == 'delete':
            query = self.query_statment.assemble_delete_statement()
            self.is_transaction = True

        else:
            query = self.query_statment.assemble_query_string()
            self.has_return = True

        query = self.options_statement.apply_options(query)

        return '{query};'.format(
            query=query
        )

    def assemble_create_table_statement(self) -> str:
        return self.table_statement.assemble_create_statement()

    def assemble_delete_table_statement(self) -> str:
        return self.table_statement.assemble_delete_statement()
