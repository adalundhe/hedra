import asyncio
from collections.abc import Iterable
from re import S
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from async_tools.functions import awaitable


class Connection:

    def __init__(self, config):

        self.config = config
        self.cluster_addresses = config.get(
            'cluster_addresses',
            ['0.0.0.0']
        )

        self.cluster = None
        self.session = None
        self.statement = None
        self.results = []

    async def connect(self) -> None:
        self.cluster = Cluster(self.cluster_addresses)
        self.session = await awaitable(self.cluster.connect)
        self.session.row_factory = dict_factory

    async def execute(self, statement, finalize=True) -> None:
        self.statement = await awaitable(self.session.prepare, statement)

        if finalize:
            response = await awaitable(self.session.execute_async, self.statement)
            result = response.result()
            self.results += list(result.all())
                

    async def commit(self) -> list:
        response = await awaitable(self.session.execute_async, self.statement)
        result = response.result()
        self.results += list(result.all())

        return self.results

    async def clear(self) -> None:
        self.results = []

    async def close(self) -> None:
        await awaitable(self.cluster.shutdown)