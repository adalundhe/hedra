import asyncio
import time
import uuid
from typing import Dict, Coroutine, Union, TypeVar, Any
from hedra.core.engines.types.common.base_engine import BaseEngine
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.http2.connection import HTTP2Connection
from hedra.core.engines.types.common.ssl import get_http2_ssl_context
from hedra.core.engines.types.common.concurrency import (
    Semaphore
)
from .pool import HTTP2Pool
from .action import HTTP2Action
from .result import HTTP2Result


A = TypeVar('A')
R = TypeVar('R')


class MercuryHTTP2Client(BaseEngine[Union[A, HTTP2Action], Union[R, HTTP2Result]]):

    __slots__ = (
        'session_id',
        'timeouts',
        '_hosts',
        'registered',
        'closed',
        'sem',
        'pool',
        'active',
        'waiter',
        'ssl_context'
    )

    def __init__(self, concurrency: int = 10**3, timeouts: Timeouts = Timeouts(), reset_connections: bool=False) -> None:
        super(
            MercuryHTTP2Client,
            self
        ).__init__()

        self.session_id = str(uuid.uuid4())
        self.timeouts = timeouts

        self._hosts = {}
        self.registered: Dict[str, HTTP2Action] = {}
        self.closed = False
        
        self.sem = Semaphore(value=concurrency)
        self.pool: HTTP2Pool = HTTP2Pool(
            concurrency, 
            self.timeouts, 
            reset_connections=reset_connections
        )

        self.pool.create_pool()
        self.active = 0
        self.waiter = None

        self.ssl_context = get_http2_ssl_context()
    
    async def set_pool(self, concurrency: int):
        self.sem = asyncio.Semaphore(value=concurrency)
        self.pool = HTTP2Pool(
            concurrency, 
            self.timeouts,
            reset_connections=self.pool.reset_connections
        )

        self.pool.create_pool()

    def extend_pool(self, increased_capacity: int):
        self.pool.size += increased_capacity
        self.pool.create_pool()
    
        self.sem = Semaphore(self.pool.size)

    def shrink_pool(self, decrease_capacity: int):
        self.pool.size -= decrease_capacity
        self.pool.create_pool()

        self.sem = Semaphore(self.pool.size)

    async def prepare(self, request: HTTP2Action) -> Coroutine[Any, Any, None]:
        try:
            request.ssl_context = self.ssl_context

            if self._hosts.get(request.url.hostname) is None:
                socket_configs = await request.url.lookup()

                for ip_addr, configs in socket_configs.items():
                        for config in configs:

                            connection = HTTP2Connection(
                                0, 
                                self.timeouts, 
                                1,
                                self.pool.reset_connections,
                                self.pool.pool_type
                            )

                            try:
                                
                                await connection.connect(
                                    request.url.hostname,
                                    ip_addr,
                                    request.url.port,
                                    config,
                                    ssl=self.ssl_context,
                                    timeout=self.timeouts.connect_timeout
                                )

                                request.url.socket_config = config
                                break

                            except Exception as e:
                                pass

                        if request.url.socket_config:
                            break

                if request.url.socket_config is None:
                        raise Exception('Err. - No socket found.')
                
                self._hosts[request.url.hostname] = request.url.ip_addr
                        
            else:
                request.url.ip_addr = self._hosts[request.url.hostname]

            if request.is_setup is False:
                request.setup()

            self.registered[request.name] = request

        except Exception as e:
            raise e

    async def execute_prepared_request(self, action: HTTP2Action) -> Coroutine[Any, Any, HTTP2Result]:
        response = HTTP2Result(action)
        response.wait_start = time.monotonic()
        self.active += 1
        
        async with self.sem:

            pipe = self.pool.pipes.pop()
            connection = self.pool.connections.pop()
        
            try:

                if action.hooks.listen:
                    event = asyncio.Event()
                    action.hooks.channel_events.append(event)
                    await event.wait()

                if action.hooks.before:
                    action: HTTP2Action = await self.execute_before(action)
                    action.setup()

                response.start = time.monotonic()

                stream = await connection.connect(
                    action.url.hostname,
                    action.url.ip_addr,
                    action.url.port,
                    action.url.socket_config,
                    ssl=action.ssl_context,
                    timeout=self.timeouts.connect_timeout
                )

                stream.encoder = action.hpack_encoder
     
                response.connect_end = time.monotonic()

                pipe.send_request_headers(action, stream)
  
                if action.encoded_data is not None:
                    await pipe.submit_request_body(action, stream)

                response.write_end = time.monotonic()

                await asyncio.wait_for(
                    pipe.receive_response(response, stream), 
                    timeout=self.timeouts.total_timeout
                )

                response.complete = time.monotonic()

                if action.hooks.after:
                    response: HTTP2Result = await self.execute_after(action, response)
                    action.setup()

                if action.hooks.notify:
                    await asyncio.gather(*[
                        asyncio.create_task(
                            channel.call(response, action.hooks.listeners)
                        ) for channel in action.hooks.channels
                    ])

                    for listener in action.hooks.listeners: 
                        if len(listener.hooks.channel_events) > 0:
                            listener.setup()
                            event = listener.hooks.channel_events.pop()
                            if not event.is_set():
                                event.set()      

                self.pool.pipes.append(pipe)
                self.pool.connections.append(connection)
                
            except Exception as e:
                response.complete = time.monotonic()
                response._status = 400
                response.error = str(e)

                self.pool.reset()

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
