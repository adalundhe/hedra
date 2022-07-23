import asyncio
from typing import Any, List


try:

    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    has_connector = True

except ImportError:
    has_connector = False


class Cassandra:

    def __init__(self, config: Any) -> None:
        self.cluster = None
        self.session = None

        self.hosts = config.hosts
        self.port = config.port or 9042
        self.username = config.username
        self.password = config.password
        self.keyspace = config.keyspace
        self._events_table_name = config.events_table
        self._metrics_table_name = config.metrics_table
        self.replication = config.replication

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

        keyspace_query = "CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : {replication} };".format(
            keyspace=self.keyspace,
            replication=self.replication
        )

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

    async def submit_events(self, events: List[Any]):
        
        for event in events:
            insert_string = f'INSERT INTO {self.keyspace}.{self._events_table_name} ({event.fields}) VALUES ({event.values});'
            
            await self._loop.run_in_executor(
                None,
                self.session.execute,
                insert_string
            )

    async def submit_metrics(self, metrics: List[Any]):

        for metric in metrics:
            insert_string = f'INSERT INTO {self.keyspace}.{self._metrics_table_name} ({metric.fields}) VALUES ({metric.values});'
            
            await self._loop.run_in_executor(
                None,
                self.session.execute,
                insert_string
            )

    async def close(self):
        await self.cluster.shutdown()