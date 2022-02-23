from __future__ import annotations
import datetime
from .models import Connection


class PrometheusConnector:

    def __init__(self, config):
        self.format = 'prometheus'
        self.config = config
        self.connection = Connection(config)

    async def connect(self) -> PrometheusConnector:
        await self.connection.connect()
        return self

    async def setup(self) -> PrometheusConnector:
        return self

    async def execute(self, data, push=True) -> PrometheusConnector:
        await self.connection.execute(data)

        if push:
            await self.connection.commit()

        return self

    async def commit(self):
        return await self.connection.commit()

    async def clear(self):
        await self.connection.clear()
        return self


    async def close(self):
        await self.connection.close()
        return self

    