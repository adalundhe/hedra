
from .types import (
    Server,
    Session
)
from .statement import Statement


class Connection:

    def __init__(self, config):
        self.config = config
        self.session = None
        self.server = None
        
    async def connect(self) -> None:
        self.session = Session(self.config)
        self.server = Server(self.config)
        await self.server.start()

    async def execute(self, query) -> None:

        statement = Statement(query)

        is_update = await statement.is_update()

        if is_update:
            await self.session.update_metrics(statement, registry=self.server.registry)
        else:
            await self.session.query_metrics(statement, registry=self.server.registry)

    async def commit(self) -> dict:

        if self.server.type == 'registry':
            await self.server.push()

        return {
            'data': self.session.results_store,
            'status': 'OK'
        }   

    async def clear(self) -> None:
        await self.session.clear_local_results_store()
        await self.session.clear_local_statement_store()

    async def close(self) -> None:
        if self.server.running:
            await self.server.stop()