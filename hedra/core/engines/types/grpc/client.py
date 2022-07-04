import time 
import asyncio
from types import FunctionType
from hedra.core.engines.types.http2 import MercuryHTTP2Client
from hedra.core.engines.types.http2.connection import HTTP2Connection
from hedra.core.engines.types.common import Timeouts, Request, Response
from hedra.core.engines.types.http2.stream import AsyncStream
from typing import Awaitable, List, Set, Tuple, Optional


GRPCResponseFuture = Awaitable[Response]
GRPCBatchResponseFuture = Awaitable[Tuple[Set[GRPCResponseFuture], Set[GRPCResponseFuture]]]


class MercuryGRPCClient(MercuryHTTP2Client):

    def __init__(self, concurrency: int = 10 ** 3, timeouts: Timeouts = None, hard_cache=False, reset_connections: bool=False) -> None:
        super(
            MercuryGRPCClient,
            self
        ).__init__(
            concurrency, 
            timeouts, 
            hard_cache, 
            reset_connections=reset_connections
        )

    async def prepare_request(self, request: Request, checks: List[FunctionType], timeout: int) -> Awaitable[None]:
        try:
            if request.url.is_ssl:
                request.ssl_context = self.ssl_context

            if self._hosts.get(request.url.hostname) is None:
                    self._hosts[request.url.hostname] = await request.url.lookup()
            else:
                request.url.ip_addr = self._hosts[request.url.hostname]

            if request.is_setup is False:
                request.setup_grpc_request(
                    grpc_request_timeout=timeout
                )

                if request.checks is None:
                    request.checks = checks

            self.requests[request.name] = request

        except Exception as e:
            return Response(request, error=e, type='grpc')

    async def update_from_context(self, request_name: str):
        previous_request = self.requests.get(request_name)
        context_request = self.context.update_request(previous_request)
        await self.prepare_request(context_request, context_request.checks)

    async def update_request(self, update_request: Request):
        previous_request = self.requests.get(update_request.name)

        previous_request.method = update_request.method
        previous_request.headers.data = update_request.headers.data
        previous_request.params.data = update_request.params.data
        previous_request.payload.data = update_request.payload.data
        previous_request.metadata.tags = update_request.metadata.tags
        previous_request.checks = update_request.checks

        self.requests[update_request.name] = previous_request

    async def execute_prepared_request(self, request_name: str, idx: int, timeout: int) -> GRPCResponseFuture:
        request = self.requests[request_name]
        response = Response(request, type='grpc')

        stream_id = idx%self.pool.size

        if request.before:
            request = await request.before(request)

        try:
            connection = self.connection_pool.connections[stream_id]
            await connection.lock.acquire()

            stream = self.pool.connections[stream_id]

            start = time.time()

            await asyncio.wait_for(stream.connect(request), self.timeouts.connect_timeout)

            connection.connect(stream)

            connection.send_request_headers(request, stream)

            await connection.submit_request_body(request, stream)
                
            await asyncio.wait_for(connection.receive_response(response, stream), timeout)

            elapsed = time.time() - start

            response.time = elapsed
            self.context.last = response

            if request.after:
                response = await request.after(idx, response)
            
            connection.lock.release()

            return response
            
        except Exception as e:
            response.response_code = 500
            response.error = e
            self.context.last = response
            self.pool.connections[stream_id] = AsyncStream(stream.stream_id, self.timeouts, self.concurrency, self.pool.reset_connections)
            self.connection_pool.connections[stream_id] = HTTP2Connection(stream_id)

            return response

    async def request(self, request: Request) -> GRPCResponseFuture:
        return await self.execute_prepared_request(request.name)

    async def batch_request(
        self, 
        request: Request,
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None
    ) -> GRPCBatchResponseFuture:

        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout
        
        return await asyncio.wait([self.execute_prepared_request(request.name, timeout) for _ in range(concurrency)], timeout=timeout)

    async def close(self) -> Awaitable[None]:
        await self.pool.close()