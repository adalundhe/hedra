from __future__ import annotations
from .models import (
    Connection,
    Statement
)


class S3Connector:

    def __init__(self, config):
        self.session = Connection(config)

    async def connect(self) -> S3Connector:
        await self.session.connect()

    async def execute(self, statement) -> S3Connector:
        statement = Statement(statement)
        await self.session.execute(statement)
        return self

    async def commit(self):
        return await self.session.commit()

    async def clear(self):
        await self.session.clear()

    async def close(self):
        await self.session.close()
