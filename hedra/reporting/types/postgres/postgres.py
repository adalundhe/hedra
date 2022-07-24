# # This is an ugly patch for: https://github.com/aio-libs/aiopg/issues/837
# import selectors  # isort:skip # noqa: F401

# selectors._PollLikeSelector.modify = (  # type: ignore
#     selectors._BaseSelectorImpl.modify  # type: ignore
# )

from typing import List
import uuid
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric


try:
    import sqlalchemy
    from sqlalchemy.dialects.postgresql import UUID
    from sqlalchemy.schema import CreateTable
    from aiopg.sa import create_engine
    from .postgres_config import PostgresConfig
    has_connector = True

except ImportError:
    UUID = None
    sqlalchemy = None
    create_engine = None
    PostgresConfig = None
    has_connector = False


class Postgres:

    def __init__(self, config: PostgresConfig) -> None:
        self.host = config.host
        self.database = config.database
        self.username = config.username
        self.password = config.password
        self.events_table_name = config.events_table
        self.metrics_table_name = config.metrics_table
        self.custom_fields = config.custom_fields
        
        self._engine = None
        self._connection = None
        self.metadata = sqlalchemy.MetaData()
        self._events_table = None
        self._metrics_table = None

    async def connect(self):
        self._engine = await create_engine(
            user=self.username,
            database=self.database,
            host=self.host,
            password=self.password
        )

        self._connection = await self._engine.acquire()

    async def submit_events(self, events: List[BaseEvent]):
        
        async with self._connection.begin() as transaction:

            for event in events:

                if self._events_table is None:
                    events_table = sqlalchemy.Table(
                        self.events_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
                        sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('time', sqlalchemy.Float),
                        sqlalchemy.Column('succeeded', sqlalchemy.Boolean),
                    )
                    
                    await self._connection.execute(CreateTable(events_table, if_not_exists=True))

                    self._events_table = events_table
                
                await self._connection.execute(self._events_table.insert(values=event.record))
            
            await transaction.commit()

    async def submit_metrics(self, metrics: List[Metric]):

        async with self._connection.begin() as transaction:

            for metric in metrics:

                if self._metrics_table is None:

                    metrics_table = sqlalchemy.Table(
                        self.metrics_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
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

                    await self._connection.execute(CreateTable(metrics_table, if_not_exists=True))

                    self._metrics_table = metrics_table

                await self._connection.execute(self._metrics_table.insert(values=metric.record))

            await transaction.commit()
        
    async def close(self):
        await self._connection.close()
        self._engine.terminate()
        await self._engine.wait_closed()



    
