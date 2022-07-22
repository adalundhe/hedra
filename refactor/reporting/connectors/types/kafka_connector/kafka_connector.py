from __future__ import annotations
from .models import Connection


class KafkaConnector:

    def __init__(self, config):
        self.config = config
        self.connection = Connection(self.config)

    async def connect(self) -> KafkaConnector:
        await self.connection.connect()
        return self

    async def setup(self) -> KafkaConnector:
        return self

    async def execute(self, query) -> KafkaConnector:
        await self.connection.execute(query)
        return self

    async def commit(self) -> list:
        return await self.connection.commit()

    async def clear(self) -> KafkaConnector:
        await self.connection.clear()
        return self

    async def close(self) -> KafkaConnector:
        await self.connection.close()
        return self
