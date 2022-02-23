import asyncio
from asyncio.tasks import sleep
import aioredis
from .response import Response


class Channel:

    def __init__(self, config, redis_connection):
        self.config = config
        self.type = config.get('channel_type', 'subscriber')
        self.channels = config.get('channels', [])
        self.options = config.get('options', {})
        self.redis_connection = redis_connection
        
        self.pubsub = None

        self._listener = None
        self._running = False
        self._encoding = self.options.get('encoding', 'utf-8')
        self._channel_data = {channel: [] for channel in self.channels}

    async def connect(self) -> None:
        self.pubsub = self.redis_connection.pubsub(
            ignore_subscribe_messages=self.options.get(
                'ignore_subscribe_messages',
                False
            )
        )

        if len(self.channels) > 0:
            self.pubsub.subscribe(*self.channels)

            for channel in self.channels:
                await self.pubsub.get_message()

    async def execute(self, statement) -> None:

        if statement.type == 'publish':
            await self.redis_connection.publish(*statement.to_record())

        elif statement.type == 'poll':
            self._listener = asyncio.create_task(self._poll(
                channel=statement.channel
            ))
            self._running = True

        elif statement.type == 'stop_poll':
            self._running = False
            await self._listener

        elif statement.type == 'subscribe':
            await self.pubsub.subscribe(statement.channel)
            await self.pubsub.get_message()

        elif statement.type == 'unsubscribe':
            await self.pubsub.unsubscribe(statement.channel)

        else:
            await self._get_message()

    async def commit(self) -> dict:
        return self._channel_data

    async def clear(self) -> None:
        for channel in self.channels:
            await self.pubsub.unsubscribe(channel)

    async def close(self) -> None:
        await self.pubsub.close()
        
        if self._listener:
            self._running = False
            await self._listener

    async def _poll(self, channel=None):

        if channel and channel not in self.channels:
            await self.pubsub.subscribe(channel)
            self.channels.append(channel)

        while self._running:
            await self._get_message()
            await asyncio.sleep(self.config.get('redis_poll_interval', 0))


    async def _get_message(self):
        raw_response = await self.pubsub.get_message()
        if raw_response:
            response = Response(
                raw_response,
                self._encoding
            )

            decoded_response = await response.decode_pubsub_response()
            channel = decoded_response.get('channel')

            if self._channel_data.get(channel):
                self._channel_data[channel].append(decoded_response)

            else:
                self._channel_data[channel] = [
                    decoded_response
                ]




            

            


    
