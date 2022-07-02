import asyncio
from types import FunctionType
import aiodns
import time
from typing import Awaitable, Dict, List, Optional, Set, Tuple, Union
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.common import Response
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
        self.requests: Dict[str, Request] = {}
        self._hosts = {}
        self._parsed_urls = {}
        self.timeouts = timeouts
        self.sem = asyncio.Semaphore(self.concurrency)
        self.loop = asyncio.get_event_loop()
        self.resolver = aiodns.DNSResolver(loop=self.loop)
        self.hard_cache = hard_cache
        self.running = False
        self.responses = []
        self.ssl_context = get_default_ssl_context()

        self.pool.create_pool()
    
    async def prepare_request(self, request: Request, checks: List[FunctionType]) -> Awaitable[None]:
        try:
            if request.url.is_ssl:
                request.ssl_context = self.ssl_context

            if request.is_setup is False:
                request.setup_http_request()

                if self._hosts.get(request.url.hostname) is None:
                    self._hosts[request.url.hostname] = await request.url.lookup()
                else:
                    request.url.ip_addr = self._hosts[request.url.hostname]

                if request.checks is None:
                    request.checks = checks

            self.requests[request.name] = request
        
        except Exception as e:
            return Response(request, error=e)

    async def execute_prepared_request(self, request_name: str, idx: int, timeout: int) -> HTTPResponseFuture:

        request = self.requests[request_name]
        response = Response(request)

        stream_idx = idx%self.pool.size

        connection = self.pool.connections[stream_idx]
        try:
            start = time.time()

            await connection.lock.acquire()
            stream = await asyncio.wait_for(connection.connect(
                request_name,
                request.url.ip_addr,
                request.url.port,
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

            response.time = elapsed
            
            connection.lock.release()
            return response

        except Exception as e:
            response.error = e
            self.pool.connections[stream_idx] = Connection(reset_connection=self.pool.reset_connections)
            return response

    async def request(self, request: Request, checks: Optional[List[FunctionType]]=[]) -> HTTPResponseFuture:

        if self.requests.get(request.name) is None:
            await self.prepare_request(request, checks)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_http_request()

        return await self.execute_prepared_request(request.name)
        
    async def batch_request(
        self, 
        request: Request,
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None,
        checks: Optional[List[FunctionType]]=[]
    ) -> HTTPBatchResponseFuture:
    
        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        if self.requests.get(request.name) is None:
            await self.prepare_request(request, checks)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_http_request()

        return await asyncio.wait([self.execute_prepared_request(request.name, idx, timeout) for idx in range(concurrency)], timeout=timeout)
