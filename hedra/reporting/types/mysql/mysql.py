import warnings
from typing import List

from numpy import float32, float64, int16, int32, int64
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet, custom_metrics_group


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

except Exception:
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
        self.groups_metrics_table_name = f'{self.metrics_table_name}_groups_metrics'
        self.errors_table_name = f'{self.metrics_table_name}_errors'
        self.custom_fields = config.custom_fields

        self._events_table = None
        self._metrics_table = None
        self._groups_metrics_table = None
        self._custom_metrics_table = {}
        self._errors_table = None

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

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        if self._groups_metrics_table is None:
            
            group_metrics_table = sa.Table(
                self.groups_metrics_table_name,
                self.metadata,
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('name', sa.VARCHAR(255)),
                sa.Column('stage', sa.VARCHAR(255)),
                sa.Column('group', sa.VARCHAR(255)),
                sa.Column('total', sa.BIGINT),
                sa.Column('succeeded', sa.BIGINT),
                sa.Column('failed', sa.BIGINT),
                sa.Column('actions_per_second', sa.FLOAT)
            )

            await self._connection.execute(CreateTable(group_metrics_table, if_not_exists=True))
            self._groups_metrics_table = group_metrics_table

        async with self._connection.begin() as transaction:

            for metrics_set in metrics_sets:
                await self._connection.execute(self._groups_metrics_table.insert(values={
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'common',
                        **metrics_set.common_stats
                    }))

            await transaction.commit()
                
    async def submit_metrics(self, metrics: List[MetricsSet]):

        async with self._connection.begin() as transaction:
        
            for metrics_set in metrics:

                if self._metrics_table is None:

                    metrics_table = sa.Table(
                        self.metrics_table_name,
                        self.metadata,
                        sa.Column('id', sa.Integer, primary_key=True),
                        sa.Column('name', sa.VARCHAR(255)),
                        sa.Column('stage', sa.VARCHAR(255)),
                        sa.Column('group', sa.TEXT()),
                        sa.Column('median', sa.FLOAT),
                        sa.Column('mean', sa.FLOAT),
                        sa.Column('variance', sa.FLOAT),
                        sa.Column('stdev', sa.FLOAT),
                        sa.Column('minimum', sa.FLOAT),
                        sa.Column('maximum', sa.FLOAT)
                    )

                    for quantile in metrics_set.quantiles:
                        metrics_table.append_column(
                            sa.Column(f'{quantile}', sa.FLOAT)
                        )

                    for custom_field_name, sql_alchemy_type in metrics_set.custom_schemas:
                        metrics_table.append_column(custom_field_name, sql_alchemy_type)

                    await self._connection.execute(CreateTable(metrics_table, if_not_exists=True))
                
                    self._metrics_table = metrics_table

                for group_name, group in metrics_set.groups.items():
                    await self._connection.execute(self._metrics_table.insert(values={
                        'group': group_name,
                        **group.record
                    }))
                    
            await transaction.commit()

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        async with self._connection.begin() as transaction:
            for metrics_set in metrics_sets:

                for custom_group_name, group in metrics_set.custom_metrics.items():
                    custom_metrics_table_name = f'{self.metrics_table_name}_{custom_group_name}'

                    if self._custom_metrics_table.get(custom_metrics_group) is None:
                        
                        custom_metrics_table = sa.Table(
                            custom_metrics_table_name,
                            self.metadata,
                            sa.Column('id', sa.Integer, primary_key=True),
                            sa.Column('name', sa.VARCHAR(255)),
                            sa.Column('stage', sa.VARCHAR(255)),
                            sa.Column('group', sa.VARCHAR(255))
                        )

                        for field, value in group.items():

                            if isinstance(value, (int, int16, int32, int64)):
                                custom_metrics_table.append_column(
                                    sa.Column(field, sa.Integer)
                                )

                            elif isinstance(value, (float, float32, float64)):
                                custom_metrics_table.append_column(
                                    sa.Column(field, sa.Float)
                                )

                        await self._connection.execute(CreateTable(custom_metrics_table, if_not_exists=True))   
                        self._custom_metrics_table[custom_metrics_table_name] = custom_metrics_table

                    await self._connection.execute(
                        self._custom_metrics_table[custom_metrics_table_name].insert(values={
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': custom_group_name,
                            **group
                        })
                    )

            await transaction.commit()


    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        async with self._connection.begin() as transaction:
            for metrics_set in metrics_sets:
                if self._errors_table is None:

                    errors_table = sa.Table(
                        self.errors_table_name,
                        self.metadata,
                        sa.Column('id', sa.Integer, primary_key=True),
                        sa.Column('name', sa.VARCHAR(255)),
                        sa.Column('stage', sa.VARCHAR(255)),
                        sa.Column('error_message', sa.TEXT),
                        sa.Column('error_count', sa.Integer)
                    ) 

                    await self._connection.execute(CreateTable(errors_table, if_not_exists=True))   
                    self._errors_table = errors_table

                for error in metrics_set.errors:
                    await self._connection.execute(self._errors_table.insert(values={
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'error_messages': error.get('message'),
                        'error_count': error.get('count')
                    }))

            await transaction.commit()

    async def close(self):
        await self._connection.close()



    


