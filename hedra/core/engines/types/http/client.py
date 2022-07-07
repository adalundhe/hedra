import asyncio
from ipaddress import ip_address
import traceback
from types import FunctionType
import aiodns
import time
from typing import Awaitable, Dict, List, Optional, Set, Tuple, Union
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.common import Response
from hedra.core.engines.types.common.context import Context
from hedra.core.engines.types.common.ssl import get_default_ssl_context
from hedra.core.engines.types.common.timeouts import Timeouts
from .connection import Connection
from .pool import Pool
from .utils import (
    http_headers_to_iterator, 
    read_body
)


HTTPResponseFuture = Awaitable[Union[Response, Exception]]
HTTPBatchResponseFuture = Awaitable[Tuple[Set[HTTPResponseFuture], Set[HTTPResponseFuture]]]


class MercuryHTTPClient:

    def __init__(self, concurrency: int = 10**3, timeouts: Timeouts = Timeouts(), hard_cache=False, reset_connections: bool=False) -> None:
        self.concurrency = concurrency
        self.pool = Pool(concurrency, reset_connections=reset_connections)
        self.registered: Dict[str, Request] = {}
        self._hosts = {}
        self._parsed_urls = {}
        self.timeouts = timeouts
        self.loop = asyncio.get_event_loop()
        self.resolver = aiodns.DNSResolver(loop=self.loop)
        self.hard_cache = hard_cache
        self.running = False
        self.responses = []
        self.ssl_context = get_default_ssl_context()
        self.context = Context()
        self._current_connection = 0

        self.pool.create_pool()

    def add_request(self, request: Request):
        self.registered[request.name] = request
        self._hosts[request.url.hostname] = request.url.hostname
    
    async def prepare(self, request: Request, checks: List[FunctionType]) -> Awaitable[None]:
        try:
            if request.url.is_ssl:
                request.ssl_context = self.ssl_context

            if self._hosts.get(request.url.hostname) is None:

                    socket_configs = await request.url.lookup()

                    for config in socket_configs:
                        try:
                            connection = Connection()
                            await connection.connect(
                                request.url.hostname,
                                request.url.ip_addr,
                                request.url.port,
                                config,
                                ssl=request.ssl_context
                            )

                            request.url.socket_config = config
                            break

                        except Exception as e:
                            pass
                
                    self._hosts[request.url.hostname] = request.url.ip_addr

            else:
                request.url.ip_addr = self._hosts[request.url.hostname]

            if request.is_setup is False:
                request.setup_http_request()

                if request.checks is None:
                    request.checks = checks

            self.registered[request.name] = request
        
        except Exception as e:
            return Response(request, error=e)

    async def execute_prepared_request(self, request: Request, idx: int, timeout: int) -> HTTPResponseFuture:

        response = Response(request, channel_id=idx)

        stream_idx = idx%self.pool.size

        connection = self.pool.connections[stream_idx]

        if request.hooks.before:
            request = await request.hooks.before(idx, request)

        try:
            await connection.lock.acquire()

            start = time.time()

            stream = await asyncio.wait_for(connection.connect(
                request.url.hostname,
                request.url.ip_addr,
                request.url.port,
                request.url.socket_config,
                ssl=request.ssl_context
            ), timeout=self.timeouts.connect_timeout)
            if isinstance(stream, Exception):
                response.error = stream
                return response

            reader, writer = stream
            writer.write(request.headers.encoded_headers)
            
            if request.payload.has_data:
                if request.payload.is_stream:
                    await request.payload.write_chunks(writer)

                else:
                    writer.write(request.payload.encoded_data)

            line = await asyncio.wait_for(reader.readuntil(), self.timeouts.socket_read_timeout)
            response.response_code = line

            async for key, value in http_headers_to_iterator(reader):
                response.headers[key] = value
            
            if response.size:
                response.body = await asyncio.wait_for(reader.readexactly(response.size), timeout)
            else:
                response = await asyncio.wait_for(read_body(reader, response), timeout)    
            
            elapsed = time.time() - start

            if request.hooks.after:
                response = await request.hooks.after(idx, response)

            response.time = elapsed
            self.context.last[request.name] = response
            connection.lock.release()
            return response

        except Exception as e:
            response.error = e
            self.pool.connections[stream_idx] = Connection(reset_connection=self.pool.reset_connections)
            self.context.last[request.name] = response
            return response

    async def request(self, request: Request) -> HTTPResponseFuture:
        return await self.execute_prepared_request(request.name)
        
    async def execute_batch(
        self, 
        request: Request,
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None
    ) -> HTTPBatchResponseFuture:
    
        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        if request.hooks.before_batch:
            request = await request.hooks.before_batch(request)
        
        responses = await asyncio.wait([self.execute_prepared_request(request, idx, timeout) for idx in range(concurrency)], timeout=timeout)
        
        if request.hooks.after_batch:
            request = await request.hooks.after_batch(request)

        return responses