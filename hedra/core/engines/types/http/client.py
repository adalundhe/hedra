import asyncio
import chunk
from inspect import trace
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
from hedra.core.engines.types.common.types import RequestTypes
from .connection import Connection
from .pool import Pool


HTTPResponseFuture = Awaitable[Union[Response, Exception]]
HTTPBatchResponseFuture = Awaitable[Tuple[Set[HTTPResponseFuture], Set[HTTPResponseFuture]]]


class MercuryHTTPClient:

    def __init__(self, concurrency: int=10**3, timeouts: Timeouts = Timeouts(), hard_cache=False, reset_connections: bool=False) -> None:
        
        self.loop = asyncio.get_event_loop()
        self.concurrency = concurrency
        self.pool = Pool(concurrency, reset_connections=reset_connections)
        self.registered: Dict[str, Request] = {}
        self._hosts = {}
        self._parsed_urls = {}
        self.timeouts = timeouts
        self.sem = asyncio.Semaphore(self.concurrency)
        self.resolver = aiodns.DNSResolver(loop=self.loop)
        self.hard_cache = hard_cache
        self.running = False
        self.responses = []
        self.context = Context()
        self.ssl_context = get_default_ssl_context()

        self.pool.create_pool()
    
    async def prepare(self, request: Request) -> Awaitable[Union[Request, Exception]]:
        try:
            if request.url.is_ssl:
                request.ssl_context = self.ssl_context

            if self._hosts.get(request.url.hostname) is None:

                    socket_configs = await request.url.lookup()
                    for ip_addr, configs in socket_configs.items():
                        for config in configs:
                            try:
                                connection = Connection()
                                await connection.make_connection(
                                    request.url.hostname,
                                    ip_addr,
                                    request.url.port,
                                    config,
                                    ssl=request.ssl_context
                                )

                                request.url.socket_config = config
                                request.url.ip_addr = ip_addr
                                request.url.has_ip_addr = True
                                break

                            except Exception as e:
                                pass

                        if request.url.socket_config:
                            break
                
                    self._hosts[request.url.hostname] = {
                        'ip_addr': request.url.ip_addr,
                        'socket_config': request.url.socket_config
                    }

                    if request.url.socket_config is None:
                        raise Exception('Err. - No socket found.')

            else:
                host_config = self._hosts[request.url.hostname]
                request.url.ip_addr = host_config.get('ip_addr')
                request.url.socket_config = host_config.get('socket_config')

            if request.is_setup is False:
                request.setup_http_request()

            self.registered[request.name] = request

            return request
        
        except Exception as e:
            raise e

    async def execute_prepared_request(self, request: Request) -> HTTPResponseFuture:
   
        response = Response(request)


        async with self.sem:
            connection = self.pool.connections.pop()
            
            try:
                if request.hooks.before:
                    request = await request.hooks.before(request)
                    request.setup_http_request()

                start = time.time()

                await connection.make_connection(
                    request.url.hostname,
                    request.url.ip_addr,
                    request.url.port,
                    request.url.socket_config,
                    timeout=self.timeouts.connect_timeout,
                    ssl=request.ssl_context
                )

                connection.write(request.headers.encoded_headers)
                
                if request.payload.has_data:
                    if request.payload.is_stream:
                        await request.payload.write_chunks(connection)

                    else:
                        connection.write(request.payload.encoded_data)

                chunk = await connection.readuntil()

                response.response_code = chunk

                headers = {}
                while True:
                    res_data = await connection.readuntil()
                    if b":" not in res_data:
                        break

                    decoded = res_data.strip()
                    pair = decoded.split(b":", 1)
        
                    key, value = pair

                    headers[key.lower()] = value.strip()

                content_length = headers.get(b'content-length')
                transfer_encoding = headers.get(b'transfer-encoding')
    
                # We require Content-Length or Transfer-Encoding headers to read a
                # request body, otherwise it's anyone's guess as to how big the body
                # is, and we ain't playing that game.
                body = bytearray()
                if content_length:
                    body = await connection.readexactly(int(content_length))

                elif transfer_encoding:
                    
                    all_chunks_read = False

                    while True and not all_chunks_read:

                        chunk_size = int((await connection.readuntil()).rstrip(), 16)
                        if not chunk_size:
                            # read last CRLF
                            body.extend(
                                await connection.readuntil()
                            )
                            break
                        
                        chunk = await connection.readexactly(chunk_size + 2)
                        body.extend(
                            chunk[:-2]
                        )

                    all_chunks_read = True

                response.headers = headers
                response.body = body
                response.time = time.time() - start
                
                self.pool.connections.append(connection)

                if request.hooks.after:
                    response = await request.hooks.after(response)
                
                return response

            except Exception as e:
                response.error = str(e)
                self.pool.connections.append(Connection(reset_connection=self.pool.reset_connections))
                return response

    async def request(self, request: Request) -> HTTPResponseFuture:
        return await self.execute_prepared_request(request.name)
        
    def execute_batch(
        self, 
        request: Request,
        concurrency: Optional[int]=None, 
        timeout: Optional[float]=None
    ) -> HTTPBatchResponseFuture:
    
        if concurrency is None:
            concurrency = self.concurrency

        if timeout is None:
            timeout = self.timeouts.total_timeout
        
        return [asyncio.create_task(self.execute_prepared_request(request, idx, timeout)) for idx in range(concurrency)]
