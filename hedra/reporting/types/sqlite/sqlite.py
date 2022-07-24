from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric


try:
    import sqlalchemy
    from sqlalchemy.schema import CreateTable
    from sqlalchemy.ext.asyncio import create_async_engine
    from .sqlite_config import SQLiteConfig
    has_connector = True

except ImportError:
    ASYNCIO_STRATEGY = None
    sqlalchemy = None
    SQLiteConfig = None
    CreateTable = None
    OperationalError = None
    has_connector = False



class SQLite:

    def __init__(self, config: SQLiteConfig) -> None:
        self.path = f'sqlite+aiosqlite:///{config.path}'
        self.events_table_name = config.events_table
        self.metrics_table_name = config.metrics_table
        self.custom_fields = config.custom_fields
        self.metadata = sqlalchemy.MetaData()
        self.database = None
        self._engine = None
        self._connection = None
        self._events_table = None
        self._metrics_table = None

    async def connect(self):
        self._engine = create_async_engine(self.path)
    
    async def submit_events(self, events: List[BaseEvent]):

        async with self._engine.begin() as connection:
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

                    
                    await connection.execute(CreateTable(events_table, if_not_exists=True))
            
                await connection.execute(self._events_table.insert().values(**event.record))
            
            await connection.commit()

    async def submit_metrics(self, metrics: List[Metric]):
        async with self._engine.begin() as connection:

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

                    await connection.execute(CreateTable(metrics_table, if_not_exists=True))

                    self._metrics_table = metrics_table

                await connection.execute(self._metrics_table.insert().values(**metric.record))

            await connection.commit()

    async def close(self):
        pass