import asyncio
import time
import uuid
from typing import Coroutine, Any
from hedra.core.engines.types.http2.client import MercuryHTTP2Client
from hedra.core.engines.types.common.timeouts import Timeouts
from .action import GraphQLHTTP2Action
from .result import GraphQLHTTP2Result


class MercuryGraphQLHTTP2Client(MercuryHTTP2Client[GraphQLHTTP2Action, GraphQLHTTP2Result]):

    def __init__(
        self, 
        concurrency: int = 10 ** 3, 
        timeouts: Timeouts = Timeouts(), 
        reset_connections: bool = False
    ) -> None:

        super(
            MercuryGraphQLHTTP2Client, 
            self
        ).__init__(
            concurrency, 
            timeouts, 
            reset_connections
        )

        self.session_id = str(uuid.uuid4())

    async def execute_prepared_request(self, action: GraphQLHTTP2Action) -> Coroutine[Any, Any, GraphQLHTTP2Result]:
        
        response = GraphQLHTTP2Result(action)
        response.wait_start = time.monotonic()
        self.active += 1

        async with self.sem:
            pipe = self.pool.pipes.pop()
            connection = self.pool.connections.pop()
        
            try:

                if action.hooks.listen:
                    event = asyncio.Event()
                    action.hooks.channel_events.append(event)
                    await event.wait()
                
                if action.hooks.before:
                    action = await self.execute_before(action)
                    action.setup()

                response.start = time.monotonic()

                stream = await connection.connect(
                    action.url.hostname,
                    action.url.ip_addr,
                    action.url.port,
                    action.url.socket_config,
                    ssl=action.ssl_context,
                    timeout=self.timeouts.connect_timeout
                )

                stream.encoder = action.hpack_encoder
     
                response.connect_end = time.monotonic()

                pipe.send_request_headers(action, stream)
  
                if action.encoded_data is not None:
                    await pipe.submit_request_body(action, stream)

                response.write_end = time.monotonic()

                await asyncio.wait_for(
                    pipe.receive_response(response, stream), 
                    timeout=self.timeouts.total_timeout
                )

                response.complete = time.monotonic()

                if action.hooks.after:
                    response = await self.execute_after(action, response)
                    action.setup()

                if action.hooks.notify:
                    await asyncio.gather(*[
                        asyncio.create_task(
                            channel.call(response, action.hooks.listeners)
                        ) for channel in action.hooks.channels
                    ])

                    for listener in action.hooks.listeners: 
                        if len(listener.hooks.channel_events) > 0:
                            listener.setup()
                            event = listener.hooks.channel_events.pop()
                            if not event.is_set():
                                event.set()      

                self.pool.pipes.append(pipe)
                self.pool.connections.append(connection)
                
            except Exception as e:
                response.response_code = 500
                response.error = str(e)

                self.pool.reset()

            self.active -= 1
            if self.waiter and self.active <= self.pool.size:

                try:
                    self.waiter.set_result(None)
                    self.waiter = None

                except asyncio.InvalidStateError:
                    self.waiter = None

            return response