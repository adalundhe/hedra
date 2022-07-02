import time
import asyncio
from types import FunctionType
from typing import Awaitable, List, Optional, Union, Tuple, Set
from async_tools.datatypes import AsyncList
from hedra.core.engines.types.http.client import MercuryHTTPClient
from hedra.core.engines.types.common.response import Response, Request
from hedra.core.engines.types.common.timeouts import Timeouts
from .utils import get_header_bits, get_message_buffer_size, websocket_headers_to_iterator


WebsocketResponseFuture = Awaitable[Union[Response, Exception]]
WebsocketBatchResponseFuture = Awaitable[Tuple[Set[WebsocketResponseFuture], Set[WebsocketResponseFuture]]]


class MercuryWebsocketClient(MercuryHTTPClient):


    def __init__(self, concurrency: int = 10 ** 3, timeouts: Timeouts = Timeouts(), hard_cache=False, reset_connection: bool=False) -> None:
        super(
            MercuryWebsocketClient,
            self
        ).__init__(concurrency, timeouts, hard_cache, reset_connections=reset_connection)

    async def prepare_request(self, request: Request, checks: List[FunctionType]) -> Awaitable[None]:
        try:
            if request.url.is_ssl:
                request.ssl_context = self.ssl_context

            if request.is_setup is False:
                request.setup_websocket_request()

                if self._hosts.get(request.url.hostname) is None:
                    self._hosts[request.url.hostname] = await request.url.lookup()
                else:
                    request.url.ip_addr = self._hosts[request.url.hostname]

                if request.checks is None:
                    request.checks = checks

            self.requests[request.name] = request

        except Exception as e:
            return Response(request, error=e, type='websocket')

    async def execute_prepared_request(self, request_name: str) -> WebsocketResponseFuture:

        request = self.requests[request_name]
        response = Response(request, type='websocket')

        await self.sem.acquire() 

        try:
            connection = self.pool.connections.pop()
            start = time.time()
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
            raw_headers = b''
            async for key, value, header_line in websocket_headers_to_iterator(reader):
                response.headers[key] = value
                raw_headers += header_line

            if request.payload.has_data:
                header_bits = get_header_bits(raw_headers)
                header_content_length = get_message_buffer_size(header_bits)
                
            if request.method == 'GET':
                response.body = await reader.read(min(16384, header_content_length))
            
            elapsed = time.time() - start

            response.time = elapsed
            
            self.pool.connections.append(connection)
            self.sem.release()

            return response

        except Exception as e:
            response.error = e
            self.sem.release()
            return response

    async def request(self, request: Request, checks: Optional[List[FunctionType]]=[]) -> WebsocketBatchResponseFuture:

        if self.requests.get(request.name) is None:

            await self.prepare_request(request, checks)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_websocket_request()

        return await self.execute_prepared_request(request.name)
        
    async def batch_request(
        self, 
        request: Request,
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None,
        checks: Optional[List[FunctionType]]=[]
    ) -> WebsocketBatchResponseFuture:

        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        if self.requests.get(request.name) is None:
            await self.prepare_request(request, checks)

        elif self.hard_cache is False:
            self.requests[request.name].update(request)
            self.requests[request.name].setup_websocket_request()
        
        return await asyncio.wait([self.execute_prepared_request(request.name) for _ in range(concurrency)], timeout=timeout)