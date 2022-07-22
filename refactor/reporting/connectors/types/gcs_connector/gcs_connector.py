from __future__ import annotations
from .models import (
    Connection,
    Statement
)


class GCSConnector:

    def __init__(self, config):
        self.session = Connection(config)

    async def connect(self) -> GCSConnector:
        await self.session.connect()

    async def execute(self, statement) -> GCSConnector:
        statement = Statement(statement)
        await self.session.execute(statement)
        return self

    async def commit(self):
        return await self.session.commit()

    async def clear(self):
        await self.session.clear()

    async def close(self):
        await self.session.close()
