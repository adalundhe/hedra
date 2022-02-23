from .types import (
    Consumer,
    Producer
)


class Session:

    def __init__(self, config):
        self.config = config
        self._producer = Producer(self.config)
        self._consumer = Consumer(self.config)

    async def connect(self) -> None:
        await self._producer.connect()
        await self._consumer.connect()

    async def execute(self, transaction) -> None:
        if transaction.type == 'update':
            await self._producer.execute(transaction.messages)
        else:
            await self._consumer.execute(transaction)

    async def commit(self) -> list:
        await self._producer.commit()
        return await self._consumer.commit()

    async def clear(self) -> None:
        await self._consumer.clear()
        await self._producer.clear()

    async def close(self) -> None:
        await self._consumer.close()
        await self._producer.close()

    