import time 
import asyncio
from types import FunctionType
from hedra.core.engines.types.http2 import MercuryHTTP2Client
from hedra.core.engines.types.http2.connection import HTTP2Connection
from hedra.core.engines.types.common import Timeouts, Request, Response
from typing import Awaitable, List, Set, Tuple, Optional
from async_tools.datatypes import AsyncList


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

            if request.is_setup is False:
                request.setup_grpc_request(
                    grpc_request_timeout=timeout
                )

                if self._hosts.get(request.url.hostname) is None:
                    self._hosts[request.url.hostname] = await request.url.lookup()
                else:
                    request.url.ip_addr = self._hosts[request.url.hostname]

                if request.checks is None:
                    request.checks = checks

            self.requests[request.name] = request

        except Exception as e:
            return Response(request, error=e, type='grpc')

    async def execute_prepared_request(self, request_name: str, timeout: int) -> GRPCResponseFuture:
        request = self.requests[request_name]
        response = Response(request, type='grpc')

        await self.sem.acquire()

        try:
            connection = self.pool.connections.pop()
            start = time.time()

            await asyncio.wait_for(connection.connect(request), self.timeouts.connect_timeout)
            stream_id = connection.get_stream_id()
            
            connection.send_request_headers(stream_id, request)

            if request.payload.has_data:
                await connection.submit_request_body(stream_id, request)
                
            await asyncio.wait_for(connection.receive_response(stream_id, response), timeout)

            elapsed = time.time() - start

            response.time = elapsed
     
            self.pool.connections.append(connection)
            self.sem.release()

            return response
        
        except Exception as e:
            response.error = e
            self.pool.connections.append(
                HTTP2Connection(reset_connection=self.pool.reset_connections)
            )

            self.sem.release()
            return response

    async def request(self, request: Request, checks: Optional[List[FunctionType]]=[]) -> GRPCResponseFuture:

        if self.requests.get(request.name) is None:
            await self.prepare_request(request, checks)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_grpc_request()

        return await self.execute_prepared_request(request.name)

    async def batch_request(
        self, 
        request: Request,
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None,
        checks: Optional[List[FunctionType]]=[]
    ) -> GRPCBatchResponseFuture:

        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        if self.requests.get(request.name) is None:
            await self.prepare_request(request, checks, timeout)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_grpc_request(grpc_request_timeout=timeout)
        
        return await asyncio.wait([self.execute_prepared_request(request.name, timeout) for _ in range(concurrency)], timeout=timeout)

    async def close(self) -> Awaitable[None]:
        await self.pool.close()