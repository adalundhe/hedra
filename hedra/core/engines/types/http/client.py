import asyncio
import traceback
import aiodns
import time
from typing import Awaitable, Dict, List, Optional, Set, Tuple, Union
from hedra.core.engines.types.common.context import Context
from hedra.core.engines.types.common.ssl import get_default_ssl_context
from hedra.core.engines.types.common.timeouts import Timeouts
from .connection import Connection
from .action import HTTPAction
from .result import HTTPResult
from .pool import Pool


HTTPResponseFuture = Awaitable[Union[HTTPResult, Exception]]
HTTPBatchResponseFuture = Awaitable[Tuple[Set[HTTPResponseFuture], Set[HTTPResponseFuture]]]


class MercuryHTTPClient:

    def __init__(self, concurrency: int=10**3, timeouts: Timeouts = Timeouts(), hard_cache=False, reset_connections: bool=False) -> None:
        
        self.loop = asyncio.get_event_loop()
        self.concurrency = concurrency
        self.pool = Pool(concurrency, reset_connections=reset_connections)
        self.registered: Dict[str, HTTPAction] = {}
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
        self._loop = asyncio.get_event_loop()

        self.pool.create_pool()
    
    async def prepare(self, action: HTTPAction) -> Awaitable[Union[HTTPAction, Exception]]:
        try:
            if action.url.is_ssl:
                action.ssl_context = self.ssl_context

            if self._hosts.get(action.url.hostname) is None:

                    socket_configs = await action.url.lookup()
                    for ip_addr, configs in socket_configs.items():
                        for config in configs:
                            try:
                                connection = Connection()
                                await connection.make_connection(
                                    action.url.hostname,
                                    ip_addr,
                                    action.url.port,
                                    config,
                                    ssl=action.ssl_context
                                )

                                action.url.socket_config = config
                                action.url.ip_addr = ip_addr
                                action.url.has_ip_addr = True
                                break

                            except Exception as e:
                                pass

                        if action.url.socket_config:
                            break
                
                    self._hosts[action.url.hostname] = {
                        'ip_addr': action.url.ip_addr,
                        'socket_config': action.url.socket_config
                    }

                    if action.url.socket_config is None:
                        raise Exception('Err. - No socket found.')

            else:
                host_config = self._hosts[action.url.hostname]
                action.url.ip_addr = host_config.get('ip_addr')
                action.url.socket_config = host_config.get('socket_config')

            if action.is_setup is False:
                action.setup()

            self.registered[action.name] = action

            return action
        
        except Exception as e:
            raise e

    async def execute_prepared_request(self, action: HTTPAction) -> HTTPResponseFuture:
   
        response = HTTPResult(action)

        response.wait_start = time.monotonic()
        async with self.sem:
            connection = self.pool.connections.pop()
            
            try:
                if action.hooks.before:
                    action = await action.hooks.before(action)
                    action.setup()

                response.start = time.monotonic()

                await connection.make_connection(
                    action.url.hostname,
                    action.url.ip_addr,
                    action.url.port,
                    action.url.socket_config,
                    timeout=self.timeouts.connect_timeout,
                    ssl=action.ssl_context
                )

                response.connect_end = time.monotonic()

                connection.write(action.encoded_headers)
                
                if action.encoded_data:
                    if action.is_stream:
                        action.write_chunks(connection)

                    else:
                        connection.write(action.encoded_data)

                response.write_end = time.monotonic()

                chunk = await connection._connection._reader.readline_fast()

                response.response_code = chunk

                headers = await connection.read_headers()

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

                response.read_end = time.monotonic()
                response.headers = headers
                response.body = body
                
                self.pool.connections.append(connection)

                if action.hooks.after:
                    response = await action.hooks.after(response)
                
                return response

            except Exception as e:
                print(traceback.format_exc())
                response.read_end = time.monotonic()
                response.error = str(e)
                self.pool.connections.append(Connection(reset_connection=self.pool.reset_connections))
                return response
