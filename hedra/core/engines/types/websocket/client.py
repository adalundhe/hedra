import time
import asyncio
import uuid
from typing import Awaitable, Dict, Union, Tuple, Set
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.common.ssl import get_default_ssl_context
from .connection import WebsocketConnection
from .pool import Pool
from .action import WebsocketAction
from .result import WebsocketResult
from .utils import get_header_bits, get_message_buffer_size


WebsocketResponseFuture = Awaitable[Union[WebsocketAction, Exception]]
WebsocketBatchResponseFuture = Awaitable[Tuple[Set[WebsocketResponseFuture], Set[WebsocketResponseFuture]]]


class MercuryWebsocketClient:

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
        'ssl_context'
    )


    def __init__(self, concurrency: int = 10 ** 3, timeouts: Timeouts = Timeouts(), reset_connections: bool=False) -> None:
        
        self.session_id = str(uuid.uuid4())
        self.timeouts = timeouts

        self.registered: Dict[str, WebsocketAction] = {}
        self._hosts = {}
        self.closed = False

        self.sem = asyncio.Semaphore(value=concurrency)
        self.pool = Pool(concurrency, reset_connections=reset_connections)
        self.pool.create_pool()
        self.active = 0
        self.waiter = None

        self.ssl_context = get_default_ssl_context()
        

        
    async def prepare(self, action: WebsocketAction) -> Awaitable[Union[WebsocketAction, Exception]]:
        try:
            if action.url.is_ssl:
                action.ssl_context = self.ssl_context

            if self._hosts.get(action.url.hostname) is None:

                    socket_configs = await action.url.lookup()
                    for ip_addr, configs in socket_configs.items():
                        for config in configs:
                            try:
                                connection = WebsocketConnection()
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

            return action
        
        except Exception as e:
            raise e

    def extend_pool(self, increased_capacity: int):
        self.pool.size += increased_capacity
        for _ in range(increased_capacity):
            self.pool.connections.append(
                WebsocketConnection(self.pool.reset_connections)
            )
        
        self.sem = asyncio.Semaphore(self.pool.size)

    def shrink_pool(self, decrease_capacity: int):
        self.pool.size -= decrease_capacity
        self.pool.connections = self.pool.connections[:self.pool.size]
        self.sem = asyncio.Semaphore(self.pool.size)


    async def execute_prepared_request(self, action: WebsocketAction) -> WebsocketResponseFuture:

        response = WebsocketResult(action)
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
                    action.name,
                    action.url.ip_addr,
                    action.url.port,
                    ssl=action.ssl_context
                )

                response.connect_end = time.monotonic()

                connection.write(action.encoded_headers)
                
                if action.encoded_data is not None:
                    if action.is_stream:
                        action.write_chunks(connection)

                    else:
                        connection.write(action.encoded_data)

                response.write_end = time.monotonic()

                line = await asyncio.wait_for(connection.readuntil(), self.timeouts.socket_read_timeout)

                response.response_code = line
                raw_headers = b''
                async for key, value, header_line in connection.iter_headers(connection):
                    response.headers[key] = value
                    raw_headers += header_line

                if action.encoded_data is not None:
                    header_bits = get_header_bits(raw_headers)
                    header_content_length = get_message_buffer_size(header_bits)
                    
                if action.method == 'GET':
                    response.body = await asyncio.wait_for(connection.readexactly(min(16384, header_content_length)), self.timeouts.total_timeout)
                
                response.complete = time.monotonic()

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

                self.pool.connections.append(connection)

            except Exception as e:
                response.complete = time.monotonic()
                response.error = str(e)

                self.pool.connections.append(
                    WebsocketConnection(reset_connection=self.pool.reset_connections) 
                )

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

