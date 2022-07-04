import asyncio
import time
import traceback
from types import FunctionType
from typing import Awaitable, Dict, List, Optional, Set, Tuple
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.http2.connection import HTTP2Connection
from hedra.core.engines.types.http2.stream import AsyncStream
from hedra.core.engines.types.common import Request, Response
from hedra.core.engines.types.common.context import Context
from hedra.core.engines.types.common.ssl import get_http2_ssl_context
from .connection_pool import ConnectionPool
from .pool import HTTP2Pool


HTTP2ResponseFuture = Awaitable[Response]
HTTP2BatchResponseFuture = Awaitable[Tuple[Set[HTTP2ResponseFuture], Set[HTTP2ResponseFuture]]]


class MercuryHTTP2Client:

    def __init__(self, concurrency: int = 10**3, timeouts: Timeouts = None, hard_cache: bool=False, reset_connections: bool=False) -> None:
        
        if timeouts is None:
            timeouts = Timeouts(connect_timeout=10)

        self.concurrency = concurrency
        self.timeouts = timeouts
        self.hard_cache = hard_cache
        self._hosts = {}
        self.requests = {}
        self.requests: Dict[str, Request] = {}
        self.ssl_context = get_http2_ssl_context()
        self.results = []
        self.iters = 0
        self.running = False
        self.pool: HTTP2Pool = HTTP2Pool(self.concurrency, self.timeouts, reset_connections=reset_connections)
        self.pool.create_pool()

        self.connection_pool = ConnectionPool(self.concurrency)
        self.connection_pool.create_pool()
        self.responses = []
        self._connected = False
        self.context = Context()

    async def prepare_request(self, request: Request, checks: List[FunctionType]) -> Awaitable[None]:
        try:
            if request.url.is_ssl:
                request.ssl_context = self.ssl_context

            if self._hosts.get(request.url.hostname) is None:
                    self._hosts[request.url.hostname] = await request.url.lookup()
            else:
                request.url.ip_addr = self._hosts[request.url.hostname]

            if request.is_setup is False:
                request.setup_http2_request()

                if request.checks is None:
                    request.checks = checks

            self.requests[request.name] = request

        except Exception as e:
            return Response(request, error=e, type='http2')

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

    async def execute_prepared_request(self, request_name: str, idx: int, timeout: int) -> HTTP2ResponseFuture:
        request = self.requests[request_name]
        response = Response(request, type='http2')

        stream_id = idx%self.pool.size

        if request.before:
            request = await request.before(idx, request)

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
                response = await request.after(response)
            
            connection.lock.release()

            return response
            
        except Exception as e:
            response.response_code = 500
            response.error = e
            self.context.last = response
            self.pool.connections[stream_id] = AsyncStream(stream.stream_id, self.timeouts, self.concurrency, self.pool.reset_connections)
            self.connection_pool.connections[stream_id] = HTTP2Connection(stream_id)
            return response

    async def request(self, request: Request) -> HTTP2ResponseFuture:
        return await self.execute_prepared_request(request.name, 0)

    async def batch_request(self, request: Request, concurrency: Optional[int]=None, timeout: Optional[float]=None) -> HTTP2BatchResponseFuture:

        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        return await asyncio.wait([self.execute_prepared_request(request.name, idx, timeout) for idx in range(concurrency)], timeout=timeout)

    async def close(self) -> Awaitable[None]:
        await self.pool.close()