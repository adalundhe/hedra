from .types import Transaction
from .session import Session

class Connection:

    def __init__(self, config):
        self.config = config
        self.session = None

    async def connect(self) -> None:
        self.session = Session(self.config)
        await self.session.connect()

    async def execute(self, queries) -> None:
        message = Transaction(queries)
        await self.session.execute(message)

    async def commit(self) -> list:
        return await self.session.commit()

    async def clear(self) -> None:
        await self.session.clear()

    async def close(self) -> None:
        await self.session.close()
        
    