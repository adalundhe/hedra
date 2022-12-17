# # This is an ugly patch for: https://github.com/aio-libs/aiopg/issues/837
# import selectors  # isort:skip # noqa: F401

# selectors._PollLikeSelector.modify = (  # type: ignore
#     selectors._BaseSelectorImpl.modify  # type: ignore
# )

import uuid
from typing import Dict, List
from numpy import float32, float64, int16, int32, int64
from hedra.logging import HedraLogger
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet


try:
    import sqlalchemy
    from sqlalchemy.dialects.postgresql import UUID
    from sqlalchemy.schema import CreateTable
    from aiopg.sa import create_engine
    from .postgres_config import PostgresConfig
    has_connector = True

except Exception:
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
        self.shared_metrics_table_name = 'stage_metrics'
        self.errors_table_name = 'stage_errors'
        self.custom_fields = config.custom_fields
        
        self._engine = None
        self._connection = None
        self.metadata = sqlalchemy.MetaData()

        self._events_table = None
        self._metrics_table = None
        self._shared_metrics_table = None
        self._errors_table = None
        self._metrics_errors_table = None
        self._custom_metrics_tables: Dict[str, sqlalchemy.Table] = {}

        self.session_uuid = str(uuid.uuid4())
        self.metadata_string: str = None
        self.logger = HedraLogger()
        self.logger.initialize()
        self.sql_type = 'Postgresql'

    async def connect(self):
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connecting to {self.sql_type} instance at - {self.host} - Database: {self.database}')
        self._engine = await create_engine(
            user=self.username,
            database=self.database,
            host=self.host,
            password=self.password
        )

        self._connection = await self._engine.acquire()

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Connected to {self.sql_type} instance at - {self.host} - Database: {self.database}')

    async def submit_events(self, events: List[BaseEvent]):
        
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Events to Table - {self.events_table_name}')

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Events to Table - {self.events_table_name} - Initiating transaction')

            for event in events:

                if self._events_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Events table - {self.events_table_name} - if not exists')

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

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Events table - {self.events_table_name}')
                
                await self._connection.execute(self._events_table.insert(values=event.record))
            
            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Events to Table - {self.events_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Events to Table - {self.events_table_name}')

    async def submit_common(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name}')

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name} - Initiating transaction')
            
            if self._shared_metrics_table is None:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Shared Metrics table - {self.shared_metrics_table_name} - if not exists')

                stage_metrics_table = sqlalchemy.Table(
                    self.shared_metrics_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
                    sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('group', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('total', sqlalchemy.BIGINT),
                    sqlalchemy.Column('succeeded', sqlalchemy.BIGINT),
                    sqlalchemy.Column('failed', sqlalchemy.BIGINT),
                    sqlalchemy.Column('actions_per_second', sqlalchemy.FLOAT)
                )

                await self._connection.execute(CreateTable(stage_metrics_table, if_not_exists=True))
                self._shared_metrics_table = stage_metrics_table

                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Shared Metrics table - {self.shared_metrics_table_name}')

            for metrics_set in metrics_sets:
                await self._connection.execute(
                    self._metrics_table.insert(values={
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': 'common',
                        **metrics_set.common_stats
                    })
                )

            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Shared Metrics to Table - {self.shared_metrics_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Shared Metrics to Table - {self.shared_metrics_table_name}')

    async def submit_metrics(self, metrics: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name}')

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name} - Initiating transaction')

            for metrics_set in metrics:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

                if self._metrics_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Metrics table - {self.metrics_table_name} - if not exists')

                    metrics_table = sqlalchemy.Table(
                        self.metrics_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
                        sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('group', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('median', sqlalchemy.FLOAT),
                        sqlalchemy.Column('mean', sqlalchemy.FLOAT),
                        sqlalchemy.Column('variance', sqlalchemy.FLOAT),
                        sqlalchemy.Column('stdev', sqlalchemy.FLOAT),
                        sqlalchemy.Column('minimum', sqlalchemy.FLOAT),
                        sqlalchemy.Column('maximum', sqlalchemy.FLOAT)
                    )

                    for quantile in metrics_set.quantiles:
                        metrics_table.append_column(
                            sqlalchemy.Column(f'{quantile}', sqlalchemy.FLOAT)
                        )

                    for custom_field_name, sql_alchemy_type in metrics_set.custom_schemas:
                        metrics_table.append_column(custom_field_name, sql_alchemy_type) 

                    await self._connection.execute(CreateTable(metrics_table, if_not_exists=True))
                    self._metrics_table = metrics_table

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Metrics table - {self.metrics_table_name}')

                for group_name, group in metrics_set.groups.items():
                    await self._connection.execute(
                        self._metrics_table.insert(values={
                            **group.record,
                            'group': group_name
                        })
                    )

            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Metrics to Table - {self.metrics_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Metrics to Table - {self.metrics_table_name}')

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics - Initiating transaction')
            
            for metrics_set in metrics_sets:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

                for custom_group_name, group in metrics_set.custom_metrics.items():

                    custom_table_name = f'{custom_group_name}_metrics'
                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Custom Metrics to table - {custom_group_name}')

                    if self._custom_metrics_tables.get(custom_table_name) is None:
                        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Custom Metrics table - {custom_group_name} - if not exists')

                        custom_metrics_table = sqlalchemy.Table(
                            custom_table_name,
                            self.metadata,
                            sqlalchemy.Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
                            sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                            sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                            sqlalchemy.Column('group', sqlalchemy.VARCHAR(255)),
                        )

                        for field, value in group.items():

                            if isinstance(value, (int, int16, int32, int64)):
                                custom_metrics_table.append_column(
                                    sqlalchemy.Column(field, sqlalchemy.Integer)
                                )

                            elif isinstance(value, (float, float32, float64)):
                                custom_metrics_table.append_column(
                                    sqlalchemy.Column(field, sqlalchemy.Float)
                                )

                        await self._connection.execute(CreateTable(custom_metrics_table, if_not_exists=True))
                        self._custom_metrics_tables[custom_table_name] = custom_metrics_table

                        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Custom Metrics table - {custom_group_name}')

                    await self._connection.execute(
                        self._custom_metrics_tables[custom_table_name].insert(values={
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': custom_group_name,
                            **group
                        })
                    )

                    await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Custom Metrics to table - {custom_group_name}')

            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Custom Metrics - Transaction committed')

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name}')

        async with self._connection.begin() as transaction:
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name} - Initiating transaction')

            for metrics_set in metrics_sets:
                await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics Set - {metrics_set.name}:{metrics_set.metrics_set_id}')

                if self._errors_table is None:
                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Creating Error Metrics table - {self.errors_table_name} - if not exists')

                    errors_table = sqlalchemy.Table(
                        self.errors_table_name,
                        self.metadata,
                        sqlalchemy.Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
                        sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                        sqlalchemy.Column('error_message', sqlalchemy.TEXT),
                        sqlalchemy.Column('error_count', sqlalchemy.Integer)
                    )  


                    await self._connection.execute(CreateTable(errors_table, if_not_exists=True))
                    self._errors_table = errors_table

                    await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Created or set Error Metrics table - {self.errors_table_name}')

                for error in metrics_set.errors:
                    await self._connection.execute(
                        self._metrics_errors_table.insert(values={
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'error_message': error.get('message'),
                            'error_count': error.get('count')
                        })
                    )

            await transaction.commit()
            await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Submitting Error Metrics to Table - {self.errors_table_name} - Transaction committed')

        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Submitted Error Metrics to Table - {self.errors_table_name}')
        
    async def close(self):
        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Closing session - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closing connectiion to {self.sql_type} at - {self.host}')

        await self._connection.close()
        self._engine.terminate()
        await self._engine.wait_closed()

        await self.logger.filesystem.aio['hedra.reporting'].debug(f'{self.metadata_string} - Session Closed - {self.session_uuid}')
        await self.logger.filesystem.aio['hedra.reporting'].info(f'{self.metadata_string} - Closed connectiion to {self.sql_type} at - {self.host}')



    
