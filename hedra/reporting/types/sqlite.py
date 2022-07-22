from typing import Any, List


try:
    import aiosqlite
    has_connector = True

except ImportError:
    has_connector = False



class SQLite:

    def __init__(self, config) -> None:
        self.path = config.path
        self.events_table = config.events_table
        self.metrics_table = config.metrics_table
        self.database = None

    async def connect(self):
        self.database = await aiosqlite.connect(self.path) 

    
    async def submit_events(self, events: List[Any]):

        for event in events:
            await self.database.execute(f'INSERT INTO {event.fields} {self.events_table} VALUES ({event.values})')

        await self.database.commit()


    async def submit_metrics(self, metrics: List[Any]):

        for metric in metrics:
            await self.database.execute(f'INSERT INTO {metric.fields} {self.metrics_table} VALUES ({metric.values})')

        await self.database.commit()

    async def close(self):
        await self.database.close()