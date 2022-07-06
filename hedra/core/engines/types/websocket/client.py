import time
import asyncio
from types import FunctionType
from typing import Awaitable, List, Optional, Union, Tuple, Set
from hedra.core.engines.types.http.client import MercuryHTTPClient
from hedra.core.engines.types.http.connection import Connection
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

    async def prepare(self, request: Request, checks: List[FunctionType]) -> Awaitable[None]:
        try:
            if request.url.is_ssl:
                request.ssl_context = self.ssl_context
            
            if self._hosts.get(request.url.hostname) is None:
                    self._hosts[request.url.hostname] = await request.url.lookup()
            else:
                request.url.ip_addr = self._hosts[request.url.hostname]

            if request.is_setup is False:
                request.setup_websocket_request()

                if request.checks is None:
                    request.checks = checks

            self.requests[request.name] = request

        except Exception as e:
            return Response(request, error=e, type='websocket')

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

    async def execute_prepared_request(self, request_name: str, idx: int, timeout: int) -> WebsocketResponseFuture:

        request = self.requests[request_name]
        response = Response(request, type='websocket')

        stream_idx = idx%self.pool.size
        connection = self.pool.connections[stream_idx]

        if request.hooks.before:
            request = await request.hooks.before(idx, request) 

        try:
            await connection.lock.acquire()

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
                response.body = await asyncio.wait_for(reader.read(min(16384, header_content_length)), timeout)
            
            elapsed = time.time() - start

            response.time = elapsed

            if request.hooks.after:
                response = await request.hooks.after(idx, response)

            self.context.last[request_name] = response
            connection.lock.release()

            return response

        except Exception as e:
            response.error = e
            self.pool.connections[stream_idx] = Connection(reset_connection=self.pool.reset_connections)  
            self.context.last[request_name] = response
            return response

    async def request(self, request: Request) -> WebsocketBatchResponseFuture:
        return await self.execute_prepared_request(request.name)
        
    async def execute_batch(
        self, 
        request: Request,
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None
    ) -> WebsocketBatchResponseFuture:

        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout

        if request.hooks.before_batch:
            request = await request.hooks.before_batch(request)
        
        responses = await asyncio.wait([self.execute_prepared_request(request.name, idx, timeout) for idx in range(concurrency)], timeout=timeout)
        
        if request.hooks.after_batch:
            request = await request.hooks.after_batch(request)

        return responses