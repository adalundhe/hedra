from __future__ import annotations

from hedra.connectors.types.cassandra_connector.models.types import keyspace_statement
from .models import (
    Connection,
    Statement
)


class CassandraConnector:

    def __init__(self, config):
        self.format = 'cassandra'
        self.config = config
        self.connection = Connection(config)
        self.keyspaces = {}
        self.tables = {}
        self.table_keyspaces = {}

    async def connect(self) -> CassandraConnector:
        await self.connection.connect()
        return self

    async def setup(self, config, finalize=True) -> CassandraConnector:
        
        for keyspace_config in config.get('keyspaces', {}):
            statement = Statement(keyspace_config)
            self.keyspaces[statement.keyspace_statement.keyspace_name] = statement

            keyspace_statement = await statement.assemble_create_keyspace_statement()
        
            await self.connection.execute(
                keyspace_statement,
                finalize=finalize
            )

        for table_config in config.get('tables', {}):
            statement = Statement(table_config)
            self.tables[statement.table_statement.table_name] = statement
            self.table_keyspaces[statement.table_statement.table_name] = statement.table_statement.keyspace

            keyspace_statement = await statement.assemble_create_table_statement()

            await self.connection.execute(
                keyspace_statement,
                finalize=finalize
            )

        return self

    async def execute(self, query, finalize=True) -> CassandraConnector:
        query = Statement(query)
        query_statement = await query.assemble_query_statement()

        await self.connection.execute(
            query_statement,
            finalize=finalize
        )
        return self

    async def commit(self, single=True) -> list:
        return await self.connection.commit()

    async def clear(self) -> CassandraConnector:

        for keyspace in self.keyspaces.values():
            keyspace_statement = await keyspace.assemble_delete_keyspace_statement()

            await self.connection.execute(
                keyspace_statement,
                finalize=True
            )

        return self

    async def close(self) -> CassandraConnector:
        await self.connection.close()
        return

