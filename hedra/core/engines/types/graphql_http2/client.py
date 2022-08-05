import asyncio
import time
from hedra.core.engines.types.http2.client import MercuryHTTP2Client
from hedra.core.engines.types.common.timeouts import Timeouts
from typing import Awaitable, Union, Tuple, Set
from .action import GraphQLHTTP2Action
from .result import GraphQLHTTP2Result

GraphQLHTTP2ResponseFuture = Awaitable[Union[GraphQLHTTP2Action, Exception]]


class MercuryGraphQLHTTP2Client(MercuryHTTP2Client):

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

    async def execute_prepared_request(self, action: GraphQLHTTP2Action) -> GraphQLHTTP2Action:
        
        response = GraphQLHTTP2Result(action)
        response.wait_start = time.monotonic()

        async with self.sem:
        
            try:

                stream = self.pool.streams.pop()
                connection = self.pool.connections.pop()
                
                if action.hooks.before:
                    action = await action.hooks.before(action, response)
                    action.setup()

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

                await connection.loop.run_in_executor(
                    connection.executor,
                    connection.send_request_headers,
                    action, 
                    reader_writer
                )

                if action.encoded_data is not None:
                    await connection.submit_request_body(action, reader_writer)

                response.write_end = time.monotonic()

                await connection.receive_response(response, reader_writer)

                response.read_end = time.monotonic()

                if action.hooks.after:
                    action = await action.hooks.after(action, response)
                    action.setup()
            
                self.pool.streams.append(stream)
                self.pool.connections.append(connection)
                
                return response
                
            except Exception as e:
                response.response_code = 500
                response.error = str(e)

                self.pool.reset()

                return response