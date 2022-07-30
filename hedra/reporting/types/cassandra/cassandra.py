import asyncio
from concurrent.futures import ThreadPoolExecutor
import functools
import os
from typing import List

import psutil
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import MetricsGroup


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

except ImportError:
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
        self._events_table_name = config.events_table
        self._metrics_table_name = config.metrics_table
        self.replication_strategy = config.replication_strategy
        self.replication = config.replication       
        self.ssl = config.ssl


        self._metrics_table = None
        self._errors_table = None
        self._events_table = None
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

        self._events_table_types = {
            'name': 'text',
            'stage': 'text',
            'time': 'float',
            'succeeded': 'boolean'
        }

        if self._events_table is None:

            self._events_table = type(
                self._events_table_name.capitalize(), 
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

    async def submit_metrics(self, metrics: List[MetricsGroup]):

        for metrics_group in metrics:
       
            if self._metrics_table is None:
                
                fields = {
                    'id': columns.UUID(primary_key=True, default=uuid.uuid4),
                    'name': columns.Text(min_length=1, index=True),
                    'stage': columns.Text(min_length=1),
                    'timings_group': columns.Text(),
                    'total': columns.Integer(),
                    'succeeded': columns.Integer(),
                    'failed': columns.Integer(),
                    'median': columns.Float(),
                    'mean': columns.Float(),
                    'variance': columns.Float(),
                    'stdev': columns.Float(),
                    'minimum': columns.Float(),
                    'maximum': columns.Float(),
                    'created_at': columns.DateTime(default=datetime.now)
                }


                for quantile_name in metrics_group.quantiles:
                    fields[quantile_name] = columns.Float()
                
                for custom_field_name, custom_field_type in metrics_group.custom_schemas:
                    fields[custom_field_name] = custom_field_type


                self._metrics_table = type(
                    self._metrics_table_name.capitalize(), 
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

            for timings_group_name, timings_group in metrics_group.groups.items():
                
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self._metrics_table.create,
                        **{
                            **timings_group.record,
                            'timings_group': timings_group_name
                        }
                    )
                )

                

    async def submit_errors(self, metrics_groups: List[MetricsGroup]):

        for metrics_group in metrics_groups:

            if self._errors_table is None:
                errors_table_fields = {
                    'id': columns.UUID(primary_key=True, default=uuid.uuid4),
                    'metric_name': columns.Text(min_length=1, index=True),
                    'metric_stage': columns.Text(min_length=1),
                    'error_message': columns.Text(min_length=1),
                    'error_count': columns.Integer()
                }

                self._errors_table = type(
                    f'{self._metrics_error_table}_errors'.capitalize()
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

            for error in metrics_group.errors:
                await self._loop.run_in_executor(
                    self._executor,
                    functools.partial(
                        self._errors_table.create,
                        {
                            'metric_name': metrics_group.name,
                            'metric_stage': metrics_group.stage,
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