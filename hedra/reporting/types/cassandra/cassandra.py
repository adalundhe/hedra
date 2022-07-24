import asyncio
from typing import List
from hedra.reporting.events.types.base_event import BaseEvent
from hedra.reporting.metric import Metric


try:

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

        self._events_table_types = {
            'name': 'text',
            'stage': 'text',
            'time': 'float',
            'succeeded': 'boolean'
        }
        
        self._metrics_table_types = {
            'name': 'text',
            'stage': 'text',
            'total': 'int',
            'succeeded': 'int',
            'failed': 'int',
            'median': 'float',
            'mean': 'float',
            'variance': 'float',
            'stdev': 'float',
            'minimum': 'float',
            'maximum': 'float',
            '0.1': 'float',
            '0.2': 'float',
            '0.25': 'float',
            '0.3': 'float',
            '0.4': 'float',
            '0.5': 'float',
            '0.6': 'float',
            '0.7': 'float',
            '0.75': 'float',
            '0.8': 'float',
            '0.9': 'float',
            '0.95': 'float',
            '0.99': 'float',
            **self.custom_fields
        }
       
        self.ssl = config.ssl
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

    async def submit_events(self, events: List[BaseEvent]):

        events_table_fields = ['id UUID PRIMARY KEY']
        for field_name, field_type in self._events_table_types.items():
            events_table_fields.append(f'{field_name} {field_type}')

        field_types = ', '.join(events_table_fields)
        create_events_table = f'CREATE TABLE IF NOT EXISTS {self.keyspace}.{self._events_table_name} ({field_types});'
        await self._loop.run_in_executor(
            None,
            self.session.execute,
            create_events_table
        )
        
        event_fields = ', '.join(events[0].fields)
        for event in events:
            event_values = []

            for value in event.values:
                if isinstance(value, str):
                    value = f'"{value}"'

                event_values.append(value)

            insert_string = f'INSERT INTO {self.keyspace}.{self._events_table_name} (id, {event_fields}) VALUES (uuid(), {event_values});'
            
            await self._loop.run_in_executor(
                None,
                self.session.execute,
                insert_string
            )

    async def submit_metrics(self, metrics: List[Metric]):

        metrics_table_fields = ['id UUID PRIMARY KEY']
        for field_name, field_type in self._metrics_table_types.items():
            metrics_table_fields.append(f'{field_name} {field_type}')

        field_types = ', '.join(metrics_table_fields)
        create_metrics_table = f'CREATE TABLE IF NOT EXISTS {self.keyspace}.{self._metrics_table_name} ({field_types});'
        await self._loop.run_in_executor(
            None,
            self.session.execute,
            create_metrics_table
        )

        metric_fields = ', '.join(metrics[0].fields)
    
        for metric in metrics:
            metric_values = []

            for value in metric.values:
                if isinstance(value, str):
                    value = f'"{value}"'

                metric_values.append(value)

            insert_string = f'INSERT INTO {self.keyspace}.{self._metrics_table_name} (id, {metric_fields}) VALUES (uuid(), {metric_values});'
            
            await self._loop.run_in_executor(
                None,
                self.session.execute,
                insert_string
            )

    async def close(self):
        await self.cluster.shutdown()