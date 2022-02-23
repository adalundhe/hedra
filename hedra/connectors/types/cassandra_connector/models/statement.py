from .types import (
    KeyspaceStatement,
    TableStatement,
    QueryStatement,
    OptionsStatment
)


class Statement:

    def __init__(self, query):
        self.keyspace_statement = KeyspaceStatement(query)
        self.table_statement = TableStatement(query)
        self.query_statment = QueryStatement(query)
        self.options_statement = OptionsStatment(
            query.get('keyspace'),
            query.get('table'), 
            query.get('options', {})
        )

    async def assemble_query_statement(self) -> str:

        query = ''

        if self.query_statment.type == 'insert':
            query = await self.query_statment.assemble_insert_statement()

        elif self.query_statment.type == 'update':
            query = await self.query_statment.assemble_update_statement()

        elif self.query_statment.type == 'delete':
            query = await self.query_statment.assemble_delete_statement()

        else:
            query = await self.query_statment.assemble_select_statement()

        query = await self.options_statement.assemble_options(query)        

        return f'{query};'

    async def assemble_create_keyspace_statement(self) -> str:
        return await self.keyspace_statement.assemble_create_statement()

    async def assemble_delete_keyspace_statement(self) -> str:
        return await self.keyspace_statement.assemble_delete_statement()

    async def assemble_create_table_statement(self) -> str:
        return await self.table_statement.assemble_create_statement()

    async def assemble_delete_table_statement(self) -> str:
        return await self.table_statement.assemble_delete_statement()
