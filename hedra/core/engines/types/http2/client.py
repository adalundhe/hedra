import asyncio
import time
import traceback
from typing import Awaitable, Dict, Set, Tuple
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.http2.stream import AsyncStream
from hedra.core.engines.types.common.ssl import get_http2_ssl_context
from .pool import HTTP2Pool
from .action import HTTP2Action
from .result import HTTP2Result


HTTP2ResponseFuture = Awaitable[HTTP2Action]
HTTP2BatchResponseFuture = Awaitable[Tuple[Set[HTTP2ResponseFuture], Set[HTTP2ResponseFuture]]]


class MercuryHTTP2Client:

    def __init__(self, concurrency: int = 10**3, timeouts: Timeouts = Timeouts(), reset_connections: bool=False) -> None:

        self.timeouts = timeouts

        self._hosts = {}
        self.registered: Dict[str, HTTP2Action] = {}

        self.ssl_context = get_http2_ssl_context()
        
        self.sem = asyncio.Semaphore(concurrency)
        self.pool: HTTP2Pool = HTTP2Pool(concurrency, self.timeouts, reset_connections=reset_connections)
        self.pool.create_pool()

    async def prepare(self, request: HTTP2Action) -> Awaitable[None]:
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
                                    1,
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
                request.setup()

            self.registered[request.name] = request

        except Exception as e:
            return e

    async def execute_prepared_request(self, request: HTTP2Action) -> HTTP2ResponseFuture:
        
        response = HTTP2Result(request)
        response.wait_start = time.monotonic()

        async with self.sem:
        
            try:

                stream = self.pool.streams.pop()
                connection = self.pool.connections.pop()
                
                if request.hooks.before:
                    request = await request.hooks.before(request)
                    request.setup()

                response.start = time.monotonic()

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

                await connection.loop.run_in_executor(
                    connection.executor,
                    connection.send_request_headers,
                    request, 
                    reader_writer
                )

                if request.encoded_data is not None:
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
                response.error = str(e)

                self.pool.reset()

                return response
