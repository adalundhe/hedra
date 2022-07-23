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
        self.events_table: config.events_table
        self.metrics_table: config.metrics_table
        self.custom_fields = config.custom_fields
        self._events_table = None
        self._metrics_table = None
        self.metadata = sqlalchemy.MetaData()
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
                events_table = sqlalchemy.Table(
                    self.events_table,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('time', sqlalchemy.Float),
                    sqlalchemy.Column('succeeded', sqlalchemy.Boolean),
                )

                await events_table.create(self._connection, checkfirst=True)
                self._events_table = events_table
            
            await self._connection.execute(
                self._events_table.insert(**event.record)
            )

    async def submit_metrics(self, metrics: List[Any]):
        for metric in metrics:

            if self._metrics_table is None:

                metrics_table = sqlalchemy.Table(
                    self.metrics_table,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('total', sqlalchemy.BIGINT),
                    sqlalchemy.Column('succeeded', sqlalchemy.BIGINT),
                    sqlalchemy.Column('failed', sqlalchemy.BIGINT),
                    sqlalchemy.Column('median', sqlalchemy.FLOAT),
                    sqlalchemy.Column('mean', sqlalchemy.FLOAT),
                    sqlalchemy.Column('variance', sqlalchemy.FLOAT),
                    sqlalchemy.Column('stdev', sqlalchemy.FLOAT),
                    sqlalchemy.Column('minimum', sqlalchemy.FLOAT),
                    sqlalchemy.Column('maximum', sqlalchemy.FLOAT)
                )

                for quantile in metric.quantiles:
                    metrics_table.append_column(
                        sqlalchemy.Column(f'{quantile}', sqlalchemy.FLOAT)
                    )

                for custom_field_name, sql_alchemy_type in self.custom_fields:
                    metrics_table.append_column(custom_field_name, sql_alchemy_type)    

                await metrics_table.create(self._connection, checkfirst=True)
                self._metrics_table = metrics_table
            
            await self._connection.execute(
                self._metrics_table.insert(**metric.record)
            )

    async def close(self):
        await self._engine.close()



    


