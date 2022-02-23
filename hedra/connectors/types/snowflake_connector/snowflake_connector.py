from __future__ import annotations
from hedra.connectors.types.postgres_connector import PostgresConnector
from async_tools.functions import awaitable
from .models import Connection

class SnowflakeConnector(PostgresConnector):

    def __init__(self, config):
        super(SnowflakeConnector, self).__init__(config)
        self.format = 'snowflake'
        self.connection = Connection(config)

    async def connect(self) -> SnowflakeConnector:
        await self.connection.connect()
        return self

    async def setup(self, tables, finalize=False) -> SnowflakeConnector:
        await awaitable(super().setup, tables, finalize=finalize)
        return self

    async def execute(self, query) -> SnowflakeConnector:
        await awaitable(super().execute, query)
        return self

    async def commit(self, single=False) -> list:
        return await awaitable(super().commit, single=single)

    async def clear(self, query) -> SnowflakeConnector:
        await awaitable(super().clear, query)
        return self

    async def close(self):
        await awaitable(super().close)
        return self

    
