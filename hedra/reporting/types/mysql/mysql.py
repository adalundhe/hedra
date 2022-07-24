import warnings
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric


try:
    import sqlalchemy as sa

    # Aiomysql will raise warnings if a table exists despite us
    # explicitly passing "IF NOT EXISTS", so we're going to
    # ignore them.
    import aiomysql
    warnings.filterwarnings('ignore', category=aiomysql.Warning)

    from aiomysql.sa import create_engine
    from sqlalchemy.schema import CreateTable
    from .mysql_config import MySQLConfig
    has_connector = True

except ImportError:
    sqlalchemy = None
    create_engine = None
    MySQLConfig = None
    CreateTable = None
    OperationalError = None
    has_connector = False



class MySQL:

    def __init__(self, config: MySQLConfig) -> None:
        self.host = config.host
        self.database = config.database
        self.username = config.username
        self.password = config.password
        self.events_table_name =  config.events_table
        self.metrics_table_name = config.metrics_table
        self.custom_fields = config.custom_fields
        self._events_table = None
        self._metrics_table = None
        self.metadata = sa.MetaData()
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

    async def submit_events(self, events: List[BaseEvent]):
        
        async with self._connection.begin() as transaction:
        
            for event in events:

                if self._events_table is None:
                    events_table = sa.Table(
                        self.events_table_name,
                        self.metadata,
                        sa.Column('id', sa.Integer, primary_key=True),
                        sa.Column('name', sa.VARCHAR(255)),
                        sa.Column('stage', sa.VARCHAR(255)),
                        sa.Column('time', sa.Float),
                        sa.Column('succeeded', sa.Boolean),
                    )

                    
                    await self._connection.execute(CreateTable(events_table, if_not_exists=True))

                    self._events_table = events_table
                
                await self._connection.execute(self._events_table.insert().values(**event.record))
                    
            await transaction.commit()

    async def submit_metrics(self, metrics: List[Metric]):

        async with self._connection.begin() as transaction:
        
            for metric in metrics:

                if self._metrics_table is None:

                    metrics_table = sa.Table(
                        self.metrics_table_name,
                        self.metadata,
                        sa.Column('id', sa.Integer, primary_key=True),
                        sa.Column('name', sa.VARCHAR(255)),
                        sa.Column('stage', sa.VARCHAR(255)),
                        sa.Column('total', sa.BIGINT),
                        sa.Column('succeeded', sa.BIGINT),
                        sa.Column('failed', sa.BIGINT),
                        sa.Column('median', sa.FLOAT),
                        sa.Column('mean', sa.FLOAT),
                        sa.Column('variance', sa.FLOAT),
                        sa.Column('stdev', sa.FLOAT),
                        sa.Column('minimum', sa.FLOAT),
                        sa.Column('maximum', sa.FLOAT)
                    )

                    for quantile in metric.quantiles:
                        metrics_table.append_column(
                            sa.Column(f'{quantile}', sa.FLOAT)
                        )

                    for custom_field_name, sql_alchemy_type in self.custom_fields:
                        metrics_table.append_column(custom_field_name, sql_alchemy_type)    

                    
                    await self._connection.execute(CreateTable(metrics_table, if_not_exists=True))

                    self._metrics_table = metrics_table

                await self._connection.execute(self._metrics_table.insert(values=metric.record))
                    
            await transaction.commit()

    async def close(self):
        await self._connection.close()



    


