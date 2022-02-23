import redis
from .response import Response


class Pipeline:

    def __init__(self, config, redis_connection):
        self.config = config
        self.redis_connection = redis_connection
        self.options = config.get('options', {})
        self._encoding = self.options.get('encoding', 'utf-8')
        self.pipeline = None

    async def connect(self) -> None:
        self.pipeline = await self.redis_connection.pipeline(
            transaction=self.options.get('enable_transactions', True)
        )

    async def execute(self, statement) -> None:

        if statement.type == 'set':
            await self.pipeline.set(*statement.to_record())

        elif statement.type == 'add_set':
            await self.pipeline.sadd(*statement.to_record())

        elif statement.type == 'get_set':
            await self.pipeline.smembers(statement.to_query())

        elif statement.type == 'get_set_size':
            await self.pipeline.scard(statement.to_query())

        elif statement.type == 'write_stream':

            location = u'{location}'.format(
                location=statement.options.get('location', '*') 
            )

            await self.pipeline.xadd(
                *statement.to_stream_record(),
                id=location
            )

        elif statement.type == 'read_stream':
            await self.pipeline.xread(*statement.to_stream_query())

        elif statement.type == 'delete':
            await self.pipeline.delete(statement.to_query())

        elif statement.type == 'find_keys':
            await self.pipeline.keys(pattern=statement.to_key_query())

        else:

            if statement.options.get('dump'):
                await self.pipeline.dump(statement.to_query())
            else:
                await self.pipeline.get(statement.to_query())

    async def commit(self) -> list:
        raw_response = await self.pipeline.execute()
        response = Response(
            raw_response,
            self._encoding
        )
        return await response.decode_pipeline_response()

    async def clear(self) -> None:
        await self.redis_connection.flushdb()


    async def close(self) -> None:
        await self.redis_connection.close()


