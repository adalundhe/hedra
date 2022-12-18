import time 
import uuid
import asyncio
from typing import Awaitable, Set, Tuple
from hedra.core.engines.types.http2 import MercuryHTTP2Client
from hedra.core.engines.types.common import Timeouts
from .action import GRPCAction
from .result import GRPCResult


GRPCResponseFuture = Awaitable[GRPCAction]
GRPCBatchResponseFuture = Awaitable[Tuple[Set[GRPCResponseFuture], Set[GRPCResponseFuture]]]


class MercuryGRPCClient(MercuryHTTP2Client):

    def __init__(self, concurrency: int = 10 ** 3, timeouts: Timeouts = None, reset_connections: bool=False) -> None:
        super(
            MercuryGRPCClient,
            self
        ).__init__(
            concurrency, 
            timeouts, 
            reset_connections=reset_connections
        )

        self.session_id = str(uuid.uuid4())

    async def execute_prepared_request(self, action: GRPCAction) -> GRPCResponseFuture:
        
        response = GRPCResult(action)
        response.wait_start = time.monotonic()
        self.active += 1

        async with self.sem:
            stream = self.pool.streams.pop()
            connection = self.pool.connections.pop()

            try:

                if action.hooks.listen:
                    event = asyncio.Event()
                    action.hooks.channel_events.append(event)
                    await event.wait()
                
                if action.hooks.before:
                    action = await action.hooks.before(action, response)
                    action.setup()

                response.start = time.monotonic()

                reader_writer = await asyncio.wait_for(stream.connect(
                    action.url.hostname,
                    action.url.ip_addr,
                    action.url.port,
                    action.url.socket_config,
                    ssl=action.ssl_context
                ), self.timeouts.connect_timeout)

                reader_writer.encoder = action.hpack_encoder
                connection.connect(reader_writer)

                response.connect_end = time.monotonic()

                connection.send_request_headers(action, reader_writer)

                if action.encoded_data is not None:
                    await connection.submit_request_body(action, reader_writer)

                response.write_end = time.monotonic()

                await connection.receive_response(response, reader_writer)

                response.complete = time.monotonic()

                if action.hooks.after:
                    action = await action.hooks.after(action, response)
                    action.setup()

                if action.hooks.notify:
                    await asyncio.gather(*[
                        asyncio.create_task(
                            channel(response, action.hooks.listeners)
                        ) for channel in action.hooks.channels
                    ])

                    for listener in action.hooks.listeners: 
                        if len(listener.hooks.channel_events) > 0:
                            listener.setup()
                            event = listener.hooks.channel_events.pop()
                            if not event.is_set():
                                event.set()  
                
                self.pool.streams.append(stream)
                self.pool.connections.append(connection)
                
            except Exception as e:
                response.response_code = 500
                response.error = e

                self.pool.reset()

            self.active -= 1
            if self.waiter and self.active <= self.pool.size:

                try:
                    self.waiter.set_result(None)
                    self.waiter = None

                except asyncio.InvalidStateError:
                    self.waiter = None

            return response
