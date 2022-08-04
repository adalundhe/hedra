import time 
import asyncio
from typing import Awaitable, Set, Tuple, Optional
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.http2 import MercuryHTTP2Client
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.types.http2.stream import AsyncStream
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

    async def execute_prepared_request(self, request: GRPCAction) -> GRPCResponseFuture:
        
        response = GRPCResult(request)
        response.wait_start = time.monotonic()

        async with self.sem:
            stream = self.pool.streams.pop()
            connection = self.pool.connections.pop()

            try:
                
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

                connection.send_request_headers(request, reader_writer)
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
                response.error = e

                self.pool.reset()

                return response
