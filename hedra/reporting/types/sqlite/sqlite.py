import uuid
from typing import List
from numpy import float32, float64, int16, int32, int64
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet


try:
    import sqlalchemy
    from sqlalchemy.schema import CreateTable
    from sqlalchemy.ext.asyncio import create_async_engine
    from .sqlite_config import SQLiteConfig
    has_connector = True

except Exception:
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
        self.shared_metrics_table_name = 'stage_metrics'
        self.errors_table_name = 'stage_errors'
        self.custom_fields = config.custom_fields
        self.metadata = sqlalchemy.MetaData()

        self.database = None
        self._engine = None
        self._connection = None

        self._events_table = None
        self._metrics_table = None
        self._shared_metrics_table = None
        self._custom_metrics_tables = {}
        self._errors_table = None

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()


    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to SQLite at - {self.path} - Database: {self.database}')
        self._engine = create_async_engine(self.path)

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to SQLite at - {self.path} - Database: {self.database}')
    
    async def submit_events(self, events: List[BaseEvent]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Table - {self.events_table_name}')

        async with self._engine.begin() as connection:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Events to Table - {self.events_table_name} - Initiating transaction')

            for event in events:

                if self._events_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Events table - {self.events_table_name} - if not exists')

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
                    self._events_table = events_table

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Events table - {self.events_table_name}')
            
                await connection.execute(self._events_table.insert(values=event.record))
            
            await connection.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Events to Table - {self.events_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Table - {self.events_table_name}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name}')

        async with self._engine.begin() as connection:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name} - Initiating transaction')

            if self._shared_metrics_table is None:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Shared Metrics table - {self.shared_metrics_table_name} - if not exists')

                stage_metrics_table = sqlalchemy.Table(
                    self.shared_metrics_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True),
                    sqlalchemy.Column('name', sqlalchemy.TEXT),
                    sqlalchemy.Column('stage', sqlalchemy.TEXT),
                    sqlalchemy.Column('group', sqlalchemy.TEXT),
                    sqlalchemy.Column('total', sqlalchemy.INTEGER),
                    sqlalchemy.Column('succeeded', sqlalchemy.INTEGER),
                    sqlalchemy.Column('failed', sqlalchemy.INTEGER),
                    sqlalchemy.Column('actions_per_second', sqlalchemy.REAL)
                )

                await connection.execute(CreateTable(stage_metrics_table, if_not_exists=True))
                self._shared_metrics_table = stage_metrics_table

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Shared Metrics table - {self.shared_metrics_table_name}')

            for metrics_set in metrics_sets:
                await connection.execute(
                    self._shared_metrics_table.insert(values={
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'common',
                        **metrics_set.common_stats
                    })
                )

            await connection.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Table - {self.shared_metrics_table_name}')

    async def submit_metrics(self, metrics: List[MetricsSet]):
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name}')

        async with self._engine.begin() as connection:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name} - Initiating transaction')

            for metrics_set in metrics:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

                if self._metrics_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Metrics table - {self.metrics_table_name} - if not exists')

                    metrics_table = sqlalchemy.Table(
                        self.metrics_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True),
                        sqlalchemy.Column('name', sqlalchemy.TEXT),
                        sqlalchemy.Column('stage', sqlalchemy.TEXT),
                        sqlalchemy.Column('group', sqlalchemy.TEXT),
                        sqlalchemy.Column('median', sqlalchemy.REAL),
                        sqlalchemy.Column('mean', sqlalchemy.REAL),
                        sqlalchemy.Column('variance', sqlalchemy.REAL),
                        sqlalchemy.Column('stdev', sqlalchemy.REAL),
                        sqlalchemy.Column('minimum', sqlalchemy.REAL),
                        sqlalchemy.Column('maximum', sqlalchemy.REAL)
                    )

                    for quantile in metrics_set.quantiles:
                        metrics_table.append_column(
                            sqlalchemy.Column(f'{quantile}', sqlalchemy.REAL)
                        )

                    for custom_field_name, sql_alchemy_type in metrics_set.custom_schemas:
                        metrics_table.append_column(custom_field_name, sql_alchemy_type)   
                    
                    await connection.execute(CreateTable(metrics_table, if_not_exists=True))
                    self._metrics_table = metrics_table

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Metrics table - {self.metrics_table_name}')

                for group_name, group in metrics_set.groups.items():
                    await connection.execute(self._metrics_table.insert(values={
                        **group.record,
                        'group': group_name
                    }))

            await connection.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Table - {self.metrics_table_name}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        async with self._engine.begin() as connection:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics - Initiating transaction')
            
            for metrics_set in metrics_sets:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

                for custom_group_name, group in metrics_set.custom_metrics.items():
                    custom_table_name = f'{custom_group_name}_metrics'

                    if self._custom_metrics_tables.get(custom_table_name) is None:
                        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Custom Metrics table - {custom_group_name} - if not exists')

                        custom_metrics_table = sqlalchemy.Table(
                            custom_table_name,
                            self.metadata,
                            sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True),
                            sqlalchemy.Column('name', sqlalchemy.TEXT),
                            sqlalchemy.Column('stage', sqlalchemy.TEXT),
                            sqlalchemy.Column('group', sqlalchemy.TEXT),
                        )

                        for field, value in group.items():

                            if isinstance(value, (int, int16, int32, int64)):
                                custom_metrics_table.append_column(
                                    sqlalchemy.Column(field, sqlalchemy.INTEGER)
                                )

                            elif isinstance(value, (float, float32, float64)):
                                custom_metrics_table.append_column(
                                    sqlalchemy.Column(field, sqlalchemy.REAL)
                                )

                        await connection.execute(CreateTable(custom_metrics_table, if_not_exists=True))
                        self._custom_metrics_tables[custom_table_name] = custom_metrics_table

                        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Custom Metrics table - {custom_group_name}')

                    await connection.execute(
                        self._custom_metrics_tables[custom_table_name].insert(values={
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': custom_group_name,
                            **group
                        })
                    )

            await connection.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics - Transaction committed')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name}')

        async with self._engine.begin() as connection:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name} - Initiating transaction')

            for metrics_set in metrics_sets:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

                if self._errors_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Error Metrics table - {self.errors_table_name} - if not exists')

                    metrics_errors_table = sqlalchemy.Table(
                        self.errors_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', sqlalchemy.INTEGER, primary_key=True),
                        sqlalchemy.Column('name', sqlalchemy.TEXT),
                        sqlalchemy.Column('stage', sqlalchemy.TEXT),
                        sqlalchemy.Column('error_message', sqlalchemy.TEXT),
                        sqlalchemy.Column('error_count', sqlalchemy.INTEGER)
                    ) 

                    await connection.execute(CreateTable(metrics_errors_table, if_not_exists=True))

                    self._errors_table = metrics_errors_table
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Error Metrics table - {self.errors_table_name}')

                for error in metrics_set.errors:
                    await connection.execute(
                        self._errors_table.insert(values={
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'error_message': error.get('message'),
                            'error_count': error.get('count')
                        })
                    )

            await connection.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Table - {self.errors_table_name}')

    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
