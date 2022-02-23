from __future__ import annotations
from os import stat

from async_tools.datatypes.async_list import AsyncList
from hedra.connectors.types.postgres_connector.models import Statement
from async_tools.functions import awaitable
from async_tools.datatypes import AsyncDict
from .models import Connection


class PostgresAsyncConnector:

    def __init__(self, config):
        self.format = 'postgres-async'
        self.config = config
        self.connection = Connection(config)
        self.tables = AsyncDict()
        self._results = AsyncList()

    async def connect(self) -> PostgresAsyncConnector:
        await self.connection.connect()
        return self
        
    async def setup(self, tables) -> PostgresAsyncConnector:
        for table_config in tables:
            table = Statement(table_config)

            self.tables[table.table_statement.table_name] = table

            create_table_statement = await awaitable(table.assemble_create_table_statement)
            await self.connection.execute(create_table_statement)

        return self

    async def execute(self, *queries) -> PostgresAsyncConnector:

        for query_dict in queries:
            query_item = AsyncDict(query_dict)
            table_name = await query_item.get('table')
            table = await self.tables.get(table_name)
            
            if table is None:
                raise Exception('Table not found!')

            field_types = table.table_statement.field_types

            query = Statement(query_dict, field_types=field_types)
            statement_string = await awaitable(query.assemble_query_statement)
            records = await self.connection.execute(
                statement_string, 
                has_return=query.has_return,
                is_transaction=query.is_transaction
            )

            if query.has_return:
                await self._results.extend(records)

        return self

    async def commit(self) -> PostgresAsyncConnector:
        records = await self._results.copy()
        await self._results.clear()
        return records.data

    async def clear(self) -> PostgresAsyncConnector:
        for table in self.tables.data.values():
            statement_string = await awaitable(table.assemble_delete_table_statement)
            await self.connection.execute(statement_string)

        return self

    async def close(self) -> PostgresAsyncConnector:
        return self
