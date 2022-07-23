from typing import Any, List


try:
    from sqlalchemy_aio import ASYNCIO_STRATEGY
    import sqlalchemy
    has_connector = True

except ImportError:
    has_connector = False



class SQLite:

    def __init__(self, config) -> None:
        self.path = config.path
        self.events_table_name = config.events_table
        self.metrics_table_name = config.metrics_table
        self.metadata = sqlalchemy.MetaData()
        self.database = None
        self.custom_fields = config.custom_fields or {}
        self._engine = None
        self._connection = None
        self._events_table = None
        self._metrics_table = None

    async def connect(self):
        # self.database = await aiosqlite.connect(self.path) 
        self._engine = sqlalchemy.create_engine(self.path, strategy=ASYNCIO_STRATEGY)
        self._connection = await self._engine.connect()
    
    async def submit_events(self, events: List[Any]):

        for event in events:

            if self._events_table is None:

                events_table = sqlalchemy.Table(
                    self.events_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True,),
                    sqlalchemy.Column('name', sqlalchemy.TEXT),
                    sqlalchemy.Column('stage', sqlalchemy.TEXT),
                    sqlalchemy.Column('time', sqlalchemy.REAL),
                    sqlalchemy.Column('succeeded', sqlalchemy.INTEGER)
                )

                await events_table.create(self._connection, checkfirst=True)
                self._events_table = events_table
           
            await self._connection.execute(
                self._events_table.insert().values(**event.record)
            )

    async def submit_metrics(self, metrics: List[Any]):

        for metric in metrics:

            if self._metrics_table is None:

                metrics_table = sqlalchemy.Table(
                    self.metrics_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True),
                    sqlalchemy.Column('name', sqlalchemy.TEXT),
                    sqlalchemy.Column('stage', sqlalchemy.TEXT),
                    sqlalchemy.Column('total', sqlalchemy.INTEGER),
                    sqlalchemy.Column('succeeded', sqlalchemy.INTEGER),
                    sqlalchemy.Column('failed', sqlalchemy.INTEGER),
                    sqlalchemy.Column('median', sqlalchemy.REAL),
                    sqlalchemy.Column('mean', sqlalchemy.REAL),
                    sqlalchemy.Column('variance', sqlalchemy.REAL),
                    sqlalchemy.Column('stdev', sqlalchemy.REAL),
                    sqlalchemy.Column('minimum', sqlalchemy.REAL),
                    sqlalchemy.Column('maximum', sqlalchemy.REAL)
                )

                for quantile in metric.quantiles:
                    metrics_table.append_column(
                        sqlalchemy.Column(f'{quantile}', sqlalchemy.REAL)
                    )

                for custom_field_name, sql_alchemy_type in self.custom_fields:
                    metrics_table.append_column(custom_field_name, sql_alchemy_type)    

                await metrics_table.create(self._connection, checkfirst=True)
                self._metrics_table = metrics_table

            await self._connection.execute(
                self._metrics_table.insert().values(**metric.record)
            )

    async def close(self):
        await self.database.close()