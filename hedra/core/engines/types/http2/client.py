import asyncio
import time
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
        self.registered = {}
        self.registered: Dict[str, Request] = {}
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

    async def prepare(self, request: Request, checks: List[FunctionType]) -> Awaitable[None]:
        try:
            request.ssl_context = self.ssl_context

            if self._hosts.get(request.url.hostname) is None:
                socket_configs = await request.url.lookup()

                for ip_addr, configs in socket_configs.items():
                        for config in configs:
                            try:
                                stream = AsyncStream(
                                    0, 
                                    self.timeouts, 
                                    self.concurrency,
                                    self.pool.reset_connections,
                                    self.pool.pool_type
                                )
                                await stream.connect(
                                    request.url.hostname,
                                    ip_addr,
                                    request.url.port,
                                    config,
                                    ssl=self.ssl_context
                                )

                                request.url.socket_config = config
                                break

                            except Exception as e:
                                pass

                        if request.url.socket_config:
                            break

                self._hosts[request.url.hostname] = request.url.ip_addr

                if request.url.socket_config is None:
                        raise Exception('Err. - No socket found.')
                        
            else:
                request.url.ip_addr = self._hosts[request.url.hostname]

            if request.is_setup is False:
                request.setup_http2_request()

                if request.checks is None:
                    request.checks = checks

            self.registered[request.name] = request

        except Exception as e:
            return e

    async def execute_prepared_request(self, request: Request, idx: int, timeout: int) -> HTTP2ResponseFuture:
        
        response = Response(request, type='http2')
        stream_id = idx%self.pool.size
        stream = self.pool.connections[stream_id]

        try:
            
            if request.hooks.before:
                request = await request.hooks.before(idx, request)

            await stream.lock.acquire()
            connection = self.connection_pool.connections[stream_id]
            
            start = time.time()

            reader_writer = await asyncio.wait_for(stream.connect(
                request.url.hostname,
                request.url.ip_addr,
                request.url.port,
                request.url.socket_config,
                ssl=request.ssl_context
            ), self.timeouts.connect_timeout)

            connection.connect(reader_writer)
            connection.send_request_headers(request, reader_writer)
            await connection.submit_request_body(request, reader_writer)
            await connection.receive_response(response, reader_writer)

            elapsed = time.time() - start

            response.time = elapsed

            if request.hooks.after:
                response = await request.hooks.after(idx, response)
            
            self.context.last[request.name] = response
            
            stream.lock.release()

            return response
            
        except Exception as e:
            response.response_code = 500
            response.error = e
            self.context.last[request.name] = response

            self.pool.connections[stream_id] = AsyncStream(
                stream.stream_id, self.timeouts, 
                self.concurrency, 
                self.pool.reset_connections,
                self.pool.pool_type
            )

            self.connection_pool.connections[stream_id] = HTTP2Connection(stream_id)
            return response

    async def request(self, request: Request) -> HTTP2ResponseFuture:
        return await self.execute_prepared_request(request, 0)

    def execute_batch(self, request: Request, concurrency: Optional[int]=None, timeout: Optional[float]=None) -> HTTP2BatchResponseFuture:

        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        return [ asyncio.create_task(
            self.execute_prepared_request(request.name, idx, timeout)
        ) for idx in range(concurrency)]

    async def close(self) -> Awaitable[None]:
        await self.pool.close()