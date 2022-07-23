import asyncio
from typing import Any, List


try:
    import sqlalchemy
    from aiomysql.sa import create_engine

    has_connector = True

except Exception:
    has_connector = False



class MySQL:

    def __init__(self, config: Any) -> None:
        self.host = config.host
        self.database = config.database
        self.username = config.username
        self.password = config.password
        self.events_table: sqlalchemy.Table = config.events_table
        self.metrics_table: sqlalchemy.Table = config.metrics_table

        self._engine = None
        self._connection = None

    async def connect(self):

        self._engine = await create_engine(
            db=self.database,
            host=self.host,
            user=self.username,
            password=self.password
        )

        self._connection = await self._engine.acquire()

    async def submit_events(self, events: List[Any]):
        for event in events:

            if self._events_table is None:
                self._events_table = event.to_table(self.events_table)
            
            await self._connection.execute(
                self._events_table.insert(**event.record)
            )

    async def submit_metrics(self, metrics: List[Any]):
        for metric in metrics:

            if self._metrics_table is None:
                self._metrics_table = metric.to_table(self.metrics_table)
            
            await self._connection.execute(
                self._metrics_table.insert(**metric.record)
            )

    async def close(self):
        await self._engine.close()



    


