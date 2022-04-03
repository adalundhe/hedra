from __future__ import annotations
from .models import Connection


class DatadogConnector:

    def __init__(self, config):
        self.config = config
        self.connection = Connection(self.config)

    async def connect(self) -> DatadogConnector:
        await self.connection.connect()
        return self

    async def setup(self) -> DatadogConnector:
        return self

    async def execute(self, query) -> DatadogConnector:
        await self.connection.execute(query)
        return self

    async def commit(self) -> list:
        return await self.connection.commit()

    async def clear(self) -> DatadogConnector:
        await self.connection.clear()
        return self

    async def close(self) -> DatadogConnector:
        await self.connection.close()
        return self