import asyncio
import time
import uuid
import traceback
from typing import Awaitable, Dict, Set, Tuple, Union
from hedra.core.engines.types.common.ssl import get_default_ssl_context
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.common.concurrency import Semaphore
from hedra.logging import HedraLogger
from .connection import HTTPConnection
from .action import HTTPAction
from .result import HTTPResult
from .pool import Pool


HTTPResponseFuture = Awaitable[Union[HTTPResult, Exception]]
HTTPBatchResponseFuture = Awaitable[Tuple[Set[HTTPResponseFuture], Set[HTTPResponseFuture]]]


class MercuryHTTPClient:

    __slots__ = (
        'session_id',
        'timeouts',
        'registered',
        '_hosts',
        'closed',
        'sem',
        'pool',
        'active',
        'waiter',
        'ssl_context',
        'logger'
    )

    def __init__(self, concurrency: int=10**3, timeouts: Timeouts = Timeouts(), reset_connections: bool=False) -> None:

        self.session_id = str(uuid.uuid4())
        self.timeouts = timeouts

        self.registered: Dict[str, HTTPAction] = {}
        self._hosts = {}
        self.closed = False

        self.sem = asyncio.Semaphore(value=concurrency)
        self.pool = Pool(concurrency, reset_connections=reset_connections)
        self.pool.create_pool()
        self.active = 0
        self.waiter = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self.ssl_context = get_default_ssl_context()

    async def wait_for_active_threshold(self):
        if self.waiter is None:
            self.waiter = asyncio.get_event_loop().create_future()
            await self.waiter


    async def prepare(self, action: HTTPAction) -> Awaitable[Union[HTTPAction, Exception]]:
        try:
            if action.url.is_ssl:
                action.ssl_context = self.ssl_context
                
            if self._hosts.get(action.url.hostname) is None:
                socket_configs = await asyncio.wait_for(action.url.lookup(), timeout=self.timeouts.connect_timeout)
            
                for ip_addr, configs in socket_configs.items():
                    for config in configs:

                        connection = HTTPConnection()
                        
                        try:
                            await connection.make_connection(
                                action.url.hostname,
                                ip_addr,
                                action.url.port,
                                config,
                                ssl=action.ssl_context,
                                timeout=self.timeouts.connect_timeout
                            )

                            action.url.socket_config = config
                            action.url.ip_addr = ip_addr
                            action.url.has_ip_addr = True
                            break

                        except Exception as e:
                            pass

                    if action.url.socket_config:
                        break

                if action.url.socket_config is None:
                    raise Exception('Err. - No socket found.')

                self._hosts[action.url.hostname] = {
                    'ip_addr': action.url.ip_addr,
                    'socket_config': action.url.socket_config
                }

            else:
                host_config = self._hosts[action.url.hostname]
                action.url.ip_addr = host_config.get('ip_addr')
                action.url.socket_config = host_config.get('socket_config')

            if action.is_setup is False:
                action.setup()

            self.registered[action.name] = action

        except Exception as e:       
            raise e

    def extend_pool(self, increased_capacity: int):
        self.pool.size += increased_capacity
        for _ in range(increased_capacity):
            self.pool.connections.append(
                HTTPConnection(self.pool.reset_connections)
            )
        
        self.sem = Semaphore(self.pool.size)

    def shrink_pool(self, decrease_capacity: int):
        self.pool.size -= decrease_capacity
        self.pool.connections = self.pool.connections[:self.pool.size]
        self.sem = Semaphore(self.pool.size)

    async def execute_prepared_request(self, action: HTTPAction) -> HTTPResponseFuture:
  
        response = HTTPResult(action)
        response.wait_start = time.monotonic()
        self.active += 1
 
        async with self.sem:
            connection = self.pool.connections.pop()
            
            try:
                
                if action.hooks.listen:
                    event = asyncio.Event()
                    action.hooks.channel_events.append(event)
                    await event.wait()

                if action.hooks.before:
                    action = await action.hooks.before(action, response)
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

                response.response_code = await connection.reader.readline_fast()
                headers = await connection.read_headers()
                status = response.status

                if status >= 300 and status < 400:
                    for _ in range(action.redirects):

                        redirect_url = headers.get(b'Location')

                        redirect_action = HTTPAction(
                            action.name,
                            redirect_url,
                            method=action.method
                        )

                        redirect_action.is_setup = True

                        self.prepare(redirect_action)

                        redirect_action.encoded_headers = action.encoded_headers
                        redirect_action.encoded_data = action.encoded_data

                        await connection.make_connection(
                            redirect_action.url.hostname,
                            redirect_action.url.ip_addr,
                            redirect_action.url.port,
                            redirect_action.url.socket_config,
                            timeout=self.timeouts.connect_timeout,
                            ssl=redirect_action.ssl_context
                        )

                        response.connect_end = time.monotonic()

                        connection.write(redirect_action.encoded_headers)
                        
                        if redirect_action.encoded_data:
                            if redirect_action.is_stream:
                                redirect_action.write_chunks(connection)

                            else:
                                connection.write(redirect_action.encoded_data)

                        response.write_end = time.monotonic()

                        response.response_code = await connection.reader.readline_fast()
                        headers = await connection.read_headers()

                        status = response.status

                        if status >= 200 and status < 300:
                            break

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
         
                response.complete = time.monotonic()
                response.headers = headers
                response.body = body
                
                self.pool.connections.append(connection)

                if action.hooks.after:
                    action = await action.hooks.after(action, response)
                    action.setup()

                if action.hooks.notify:
                    await asyncio.gather(*[
                        asyncio.create_task(
                            channel(response, action.hooks.listeners)
                        ) for channel in action.hooks.channels
                    ])

                    for listener in action.hooks.listeners: 
                        if len(listener.hooks.channel_events) > 0:
                            listener.setup()
                            event = listener.hooks.channel_events.pop()
                            if not event.is_set():
                                event.set()       

            except Exception as e:
                response.complete = time.monotonic()
                response.error = str(e)

                self.pool.connections.append(HTTPConnection(reset_connection=self.pool.reset_connections))

            self.active -= 1
            if self.waiter and self.active <= self.pool.size:

                try:
                    self.waiter.set_result(None)
                    self.waiter = None

                except asyncio.InvalidStateError:
                    self.waiter = None

            return response

    async def close(self):
        if self.closed is False:
            await self.pool.close()
            self.closed = True
