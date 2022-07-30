import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List

import psutil
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup


try:
    import sqlalchemy
    from snowflake.sqlalchemy import URL
    from sqlalchemy import create_engine
    from sqlalchemy.schema import CreateTable
    from .snowflake_config import SnowflakeConfig
    has_connector = True

except Exception:
    snowflake = None
    SnowflakeConfig = None
    has_connector = False

class Snowflake:

    def __init__(self, config: SnowflakeConfig) -> None:
        self.username = config.username
        self.password = config.password
        self.organization_id = config.organization_id
        self.account_id = config.account_id
        self.private_key = config.private_key
        self.warehouse = config.warehouse
        self.database = config.database
        self.schema = config.database_schema
        self.events_table_name = config.events_table
        self.metrics_table_name = config.metrics_table
        self.errors_table_name = f'{self.metrics_table_name}_errors'
        self.custom_fields = config.custom_fields
        self.connect_timeout = config.connect_timeout
        
        self.metadata = sqlalchemy.MetaData()
        self._executor = ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))

        self._engine = None
        self._connection = None
        self._events_table = None
        self._metrics_table = None
        self._errors_table = None
        self._loop = asyncio.get_event_loop()

    async def connect(self):

        try:
            self._engine = await self._loop.run_in_executor(
                self._executor,
                create_engine,
                URL(
                    user=self.username,
                    password=self.password,
                    account=self.account_id,
                    warehouse=self.warehouse,
                    database=self.database,
                    schema=self.schema
                )
            
            )

            self._connection = await asyncio.wait_for(
                self._loop.run_in_executor(
                    self._executor,
                    self._engine.connect
                ),
                timeout=self.connect_timeout
            )

        except asyncio.TimeoutError:
            raise Exception('Err. - Connection to Snowflake timed out - check your account id, username, and password.')


    async def submit_events(self, events: List[BaseEvent]):
    

        for event in events:

            if self._events_table is None:
                events_table = sqlalchemy.Table(
                    self.events_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('time', sqlalchemy.Float),
                    sqlalchemy.Column('succeeded', sqlalchemy.Boolean),
                )
                
                await self._loop.run_in_executor(
                    self._executor,
                    self._connection.execute,
                    CreateTable(events_table, if_not_exists=True)
                )

                self._events_table = events_table
            
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                self._events_table.insert(values=event.record)
            )
        

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        for metrics_group in metrics:

            if self._metrics_table is None:

                metrics_table = sqlalchemy.Table(
                    self.metrics_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('stage', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('timings_group', sqlalchemy.VARCHAR(255)),
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

                for quantile in metrics_group.quantiles:
                    metrics_table.append_column(
                        sqlalchemy.Column(f'{quantile}', sqlalchemy.FLOAT)
                    )

                for custom_field_name, sql_alchemy_type in metrics_group.custom_schemas:
                    metrics_table.append_column(custom_field_name, sql_alchemy_type)  

                
                await self._loop.run_in_executor(
                    self._executor,
                    self._connection.execute,
                    CreateTable(metrics_table, if_not_exists=True)
                )

                self._metrics_table = metrics_table

        for timings_group_name, timings_group in metrics_group.groups.items():
            await self._loop.run_in_executor(
                self._executor,
                self._connection.execute,
                self._metrics_table.insert(values={
                    **timings_group.record,
                    'timings_group': timings_group_name
                })
            )

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):

        for metrics_group in metrics_groups:

            if self._errors_table is None:
                errors_table = sqlalchemy.Table(
                    self.errors_table_name,
                    self.metadata,
                    sqlalchemy.Column('id', sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column('metric_name', sqlalchemy.VARCHAR(255)),
                    sqlalchemy.Column('metrics_stage', sqlalchemy.TEXT),
                    sqlalchemy.Column('error_message', sqlalchemy.TEXT),
                    sqlalchemy.Column('error_count', sqlalchemy.BIGINT)
                )

                await self._loop.run_in_executor(
                    self._executor,
                    self._connection.execute,
                    CreateTable(errors_table, if_not_exists=True)
                )

                self._errors_table = errors_table

            for error in metrics_group.errors:
                await self._loop.run_in_executor(
                    self._executor,
                    self._connection.execute,
                    self._errors_table.insert(values={
                        'metric_name': metrics_group.name,
                        'metric_stage': metrics_group.stage,
                        'error_message': error.get('message'),
                        'error_count': error.get('count')
                    })
                )
        
    async def close(self):
        await self._loop.run_in_executor(
            None,
            self._connection.close
        )