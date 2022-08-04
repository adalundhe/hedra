import asyncio
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import functools
import os
from typing import List
from numpy import float32, float64, int16, int32, int64

import psutil
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsSet


try:
    import uuid
    from cassandra.cqlengine import columns
    from cassandra.cqlengine import connection
    from datetime import datetime
    from cassandra.cqlengine.management import sync_table
    from cassandra.cqlengine.models import Model
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from .cassandra_config import CassandraConfig
    has_connector = True

except Exception:
    Cluster = None
    PlainTextAuthProvider = None
    CassandraConfig = None
    has_connector = False


class Cassandra:

    def __init__(self, config: CassandraConfig) -> None:
        self.cluster = None
        self.session = None

        self.hosts = config.hosts
        self.port = config.port or 9042
        self.username = config.username
        self.password = config.password
        self.keyspace = config.keyspace
        self.custom_fields = config.custom_fields
        self.events_table_name: str = config.events_table
        self.metrics_table_name: str = config.metrics_table
        self.stage_metrics_table_name = 'stage_metrics'
        self.custom_metrics_table_names = {}
        self.errors_table_name = 'stage_errors'
        self.replication_strategy = config.replication_strategy
        self.replication = config.replication       
        self.ssl = config.ssl


        self._metrics_table = None
        self._errors_table = None
        self._events_table = None
        self._stage_metrics_table = None
        self._custom_metrics_tables = {}

        self._executor =ThreadPoolExecutor(max_workers=psutil.cpu_count(logical=False))
        self._loop = asyncio.get_event_loop()

    async def connect(self):

        auth = None
        if self.username and self.password:
            auth = PlainTextAuthProvider(self.username, self.password)
        

        self.cluster = Cluster(
            self.hosts,
            port=self.port,
            auth_provider=auth,
            ssl_context=self.ssl
        )

        self.session = await self._loop.run_in_executor(
            None,
            self.cluster.connect
        )

        if self.keyspace is None:
            self.keyspace = 'hedra'

        keyspace_options = f"'class' : '{self.replication_strategy}', 'replication_factor' : {self.replication}"
        keyspace_query = f"CREATE KEYSPACE IF NOT EXISTS {self.keyspace} WITH REPLICATION = " + "{" + keyspace_options  + "};"

        await self._loop.run_in_executor(
            None,
            self.session.execute,
            keyspace_query
        )

        await self._loop.run_in_executor(
            None,
            self.session.set_keyspace,
            self.keyspace
        )
        if os.getenv('CQLENG_ALLOW_SCHEMA_MANAGEMENT') is None:
            os.environ['CQLENG_ALLOW_SCHEMA_MANAGEMENT'] = '1'

        await self._loop.run_in_executor(
            self._executor,
            functools.partial(
                connection.setup,
                self.hosts, 
                self.keyspace, 
                protocol_version=3
            )
        )

    async def submit_events(self, events: List[BaseEvent]):

        if self._events_table is None:

            self._events_table = type(
                self.events_table_name.capitalize(), 
                (Model, ), 
                {
                    'id': columns.UUID(primary_key=True, default=uuid.uuid4),
                    'name': columns.Text(min_length=1, index=True),
                    'stage': columns.Text(min_length=1),
                    'time': columns.Float(),
                    'succeeded': columns.Boolean(),
                    'created_at': columns.DateTime(default=datetime.now)
                }
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    sync_table,
                    self._events_table,
                    keyspaces=[self.keyspace]
                )
            )

        for event in events:
            
            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    self._metrics_table.create,
                    **event.record
                )
            )

    async def submit_common(self, metrics_sets: List[MetricsSet]):
        
        if self._stage_metrics_table is None:

            fields = {
                'id': columns.UUID(primary_key=True, default=uuid.uuid4),
                'name': columns.Text(min_length=1, index=True),
                'stage': columns.Text(min_length=1),
                'group': columns.Text(min_length=1),
                'total': columns.Integer(),
                'succeeded': columns.Integer(),
                'failed': columns.Integer(),
                'actions_per_second': columns.Float()
            }

            self._stage_metrics_table = type(
                self.stage_metrics_table_name.capitalize(), 
                (Model, ), 
                fields
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    sync_table,
                    self._stage_metrics_table,
                    keyspaces=[self.keyspace]
                )
            )

        for metrics_set in metrics_sets:
            
            await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self._metrics_table.create,
                        **{
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'group': 'common',
                            **metrics_set.common_stats
                        }
                    )
                )

    async def submit_metrics(self, metrics: List[MetricsSet]):

        for metrics_set in metrics:
       
            if self._metrics_table is None:
                
                fields = {
                    'id': columns.UUID(primary_key=True, default=uuid.uuid4),
                    'name': columns.Text(min_length=1, index=True),
                    'stage': columns.Text(min_length=1),
                    'group': columns.Text(),
                    'median': columns.Float(),
                    'mean': columns.Float(),
                    'variance': columns.Float(),
                    'stdev': columns.Float(),
                    'minimum': columns.Float(),
                    'maximum': columns.Float(),
                    'created_at': columns.DateTime(default=datetime.now)
                }


                for quantile_name in metrics_set.quantiles:
                    fields[quantile_name] = columns.Float()
                
                for custom_field_name, custom_field_type in metrics_set.custom_schemas:
                    fields[custom_field_name] = custom_field_type


                self._metrics_table = type(
                    self.metrics_table_name.capitalize(), 
                    (Model, ), 
                    fields
                )

                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        sync_table,
                        self._metrics_table,
                        keyspaces=[self.keyspace]
                    )
                )

            for group_name, group in metrics_set.groups.items():
                
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self._metrics_table.create,
                        **{
                            **group.record,
                            'group': group_name
                        }
                    )
                )

    async def submit_custom(self, metrics_sets: List[MetricsSet]):

        for metrics_set in metrics_sets:
            
            for custom_group_name, group in metrics_set.custom_metrics.items():

                custom_metrics_table_name = f'{custom_group_name}_metrics'
                if self._custom_metrics_tables.get(custom_metrics_table_name):
                    
                    custom_table = {
                        'id': columns.UUID(primary_key=True, default=uuid.uuid4),
                        'name': columns.Text(min_length=1, index=True),
                        'stage': columns.Text(min_length=1),
                        'group': columns.Text(min_length=1)
                    }

                    for field_name, value in group.items():

                        cassandra_column = None
                        if isinstance(value, (int, int16, int32, int64)):
                            cassandra_column = columns.Integer()
                        
                        elif isinstance(value, (float, float32, float64)):
                            cassandra_column = columns.Float()

                        custom_table[field_name] = cassandra_column

                    custom_metrics_table = type(
                        custom_metrics_table_name.capitalize(), 
                        (Model, ), 
                        custom_table
                    )

                    await self._loop.run_in_executor(
                        self._executor,
                        functools.partial(
                            sync_table,
                            custom_metrics_table,
                            keyspaces=[self.keyspace]
                        )
                    )

                    self._custom_metrics_tables[custom_metrics_table_name] = custom_metrics_table
    
                table = self._custom_metrics_tables.get(custom_metrics_table_name)
                await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    table.create,
                    {
                        'name': metrics_set.name,
                        'stage': metrics_set.stage,
                        'group': custom_group_name,
                        **group
                    }
                )
            )

    async def submit_errors(self, metrics_sets: List[MetricsSet]):

        if self._errors_table is None:
            errors_table_fields = {
                'id': columns.UUID(primary_key=True, default=uuid.uuid4),
                'name': columns.Text(min_length=1, index=True),
                'stage': columns.Text(min_length=1),
                'error_message': columns.Text(min_length=1),
                'error_count': columns.Integer()
            }

            self._errors_table = type(
                self.errors_table_name.capitalize()
                (Model, ),
                errors_table_fields
            )

            await self._loop.run_in_executor(
                self._executor,
                functools.partial(
                    sync_table,
                    self._errors_table,
                    keyspaces=[self.keyspace]
                )
            )

        for metrics_set in metrics_sets:

            for error in metrics_set.errors:
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self._errors_table.create,
                        {
                            'name': metrics_set.name,
                            'stage': metrics_set.stage,
                            'error_message': error.get('message'),
                            'error_count': error.get('count')
                        }
                    )
                )
 
    async def close(self):
        await self._loop.run_in_executor(
            None,
            self.cluster.shutdown
        )