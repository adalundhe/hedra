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

    async def execute_prepared_request(self, request: GraphQLHTTP2Action) -> GraphQLHTTP2Action:
        
        response = GraphQLHTTP2Result(request)
        response.wait_start = time.monotonic()

        async with self.sem:
        
            try:

                stream = self.pool.streams.pop()
                connection = self.pool.connections.pop()
                
                if request.hooks.before:
                    request = await request.hooks.before(request)
                    request.setup()

                reader_writer = await asyncio.wait_for(stream.connect(
                    request.url.hostname,
                    request.url.ip_addr,
                    request.url.port,
                    request.url.socket_config,
                    ssl=request.ssl_context
                ), self.timeouts.connect_timeout)

                reader_writer.encoder = request.hpack_encoder
                connection.connect(reader_writer)

                response.connect_end = time.monotonic()

                await connection.loop.run_in_executor(
                    connection.executor,
                    connection.send_request_headers,
                    request, 
                    reader_writer
                )

                if request.encoded_data is not None:
                    await connection.submit_request_body(request, reader_writer)

                response.write_end = time.monotonic()

                await connection.receive_response(response, reader_writer)

                response.read_end = time.monotonic()

                if request.hooks.after:
                    response = await request.hooks.after(response)
            
                self.pool.streams.append(stream)
                self.pool.connections.append(connection)
                
                return response
                
            except Exception as e:
                response.response_code = 500
                response.error = str(e)

                self.pool.reset()

                return response