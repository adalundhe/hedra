import asyncio
import time
import uuid
from typing import (
    Dict, 
    Coroutine, 
    Union, 
    TypeVar, 
    Any, 
    List,
    Iterator,
    Optional
)
from hedra.core_rewrite.engines.client.client_types.common.base_client import BaseClient
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.http2.connection import HTTP2Connection
from hedra.core.engines.types.common.ssl import get_http2_ssl_context
from hedra.core.engines.types.common.concurrency import Semaphore
from hedra.core.engines.types.tracing.trace_session import (
    TraceSession, 
    Trace
)

from .pool import HTTP2Pool
from .action import HTTP2Action
from .result import HTTP2Result


A = TypeVar('A')
R = TypeVar('R')


class HTTP2Client(BaseClient[Union[A, HTTP2Action], Union[R, HTTP2Result]]):

    __slots__ = (
        'mutations',
        'actions',
        'initialized',
        'suspend',
        'session_id',
        'next_name',
        'timeouts',
        '_hosts',
        'registered',
        'closed',
        'sem',
        'pool',
        'active',
        'waiter',
        'ssl_context',
        'tracing_session'
    )

    def __init__(
        self, 
        concurrency: int = 10**3, 
        timeouts: Timeouts = Timeouts(), 
        reset_connections: bool=False,
        tracing_session: Optional[TraceSession]=None
    ) -> None:
        super(
            HTTP2Client,
            self
        ).__init__()

        self.session_id = str(uuid.uuid4())
        self.timeouts = timeouts

        self._hosts = {}
        self.registered: Dict[str, HTTP2Action] = {}
        self.closed = False
        self.suspend: bool = False
        self.initialized: bool = False
        
        self.sem = Semaphore(value=concurrency)
        self.pool: HTTP2Pool = HTTP2Pool(
            concurrency, 
            self.timeouts, 
            reset_connections=reset_connections
        )
        self.tracing_session: Union[TraceSession, None] = tracing_session

        self.pool.create_pool()
        self.active = 0
        self.waiter = None
        self.next_name: Union[str, None] = None

        self.ssl_context = get_http2_ssl_context()

    def config_to_dict(self):
        return {
            'concurrency': self.pool.size,
            'timeouts': {
                'connect_timeout': self.timeouts.connect_timeout,
                'socket_read_timeout': self.timeouts.socket_read_timeout,
                'total_timeout': self.timeouts.total_timeout
            },
            'reset_connections': self.pool.reset_connections
        }
    
    async def get(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        user: str = None,
        tags: List[Dict[str, str]] = [],
        trace: Trace=None
    ):
        if trace and self.tracing_session is None:
            self.tracing_session = TraceSession(
                **trace.to_dict()
            )

        request = HTTP2Action(
            self.next_name,
            url,
            method='GET',
            headers=headers,
            data=None,
            user=user,
            tags=tags
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return request
        
        return await self.request(request)

    async def post(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        trace: Trace=None
    ):
        if trace and self.tracing_session is None:
            self.tracing_session = TraceSession(
                **trace.to_dict()
            )

        request = HTTP2Action(
            self.next_name,
            url,
            method='POST',
            headers=headers,
            data=data,
            user=user,
            tags=tags
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return request
    
        return await self.request(request)


    async def put(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        trace: Trace=None
    ):
        if trace and self.tracing_session is None:
            self.tracing_session = TraceSession(
                **trace.to_dict()
            )

        request = HTTP2Action(
            self.next_name,
            url,
            method='PUT',
            headers=headers,
            data=data,
            user=user,
            tags=tags
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return request

        return await self.request(request)


    async def patch(
        self,
        url: str, 
        headers: Dict[str, str] = {}, 
        data: Union[dict, str, bytes, Iterator] = None,
        user: str = None,
        tags: List[Dict[str, str]] = [],
        trace: Trace=None
    ):
        if trace and self.tracing_session is None:
            self.tracing_session = TraceSession(
                **trace.to_dict()
            )

        request = HTTP2Action(
            self.next_name,
            url,
            method='PATCH',
            headers=headers,
            data=data,
            user=user,
            tags=tags
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return request

        return await self.request(request)


    async def delete(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        user: str = None,
        tags: List[Dict[str, str]] = [],
        trace: Trace=None
    ):
        if trace and self.tracing_session is None:
            self.tracing_session = TraceSession(
                **trace.to_dict()
            )

        request = HTTP2Action(
            self.next_name,
            url,
            method='DELETE',
            headers=headers,
            data=None,
            user=user,
            tags=tags
        )

        if self.suspend and self.waiter is None:
            self.waiter = asyncio.Future()
            await self.waiter

            return request

        return await self.request(request)

    async def prepare(
        self, 
        action: HTTP2Action
    ) -> Coroutine[Any, Any, None]:
        try:
            action.ssl_context = self.ssl_context

            if self._hosts.get(action.url.hostname) is None:
                socket_configs = await action.url.lookup()

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
                                    action.url.hostname,
                                    ip_addr,
                                    action.url.port,
                                    config,
                                    ssl=self.ssl_context,
                                    timeout=self.timeouts.connect_timeout
                                )

                                action.url.socket_config = config
                                break

                            except Exception as e:
                                pass

                        if action.url.socket_config:
                            break

                if action.url.socket_config is None:
                        raise Exception('Err. - No socket found.')
                
                self._hosts[action.url.hostname] = action.url.ip_addr
                        
            else:
                action.url.ip_addr = self._hosts[action.url.hostname]

            if action.is_setup is False:
                action.setup()

            self.registered[action.name] = action

        except Exception as e:
            raise e

    async def request(self, action: HTTP2Action) -> Coroutine[Any, Any, HTTP2Result]:

        trace: Union[Trace, None] = None
        if self.tracing_session:
            trace = self.tracing_session.create_trace()
            await trace.on_request_start(action)

        response = HTTP2Result(action)
        response.wait_start = time.monotonic()
        self.active += 1

        if trace and trace.on_connection_queued_start:
            await trace.on_connection_queued_start(
                trace.span,
                action,
                response
            )
        
        async with self.sem:

            if trace and trace.on_connection_queued_end:
                await trace.on_connection_queued_end(
                    trace.span,
                    action,
                    response
                )

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

                if trace and trace.on_connection_create_start:
                    await trace.on_connection_create_start(
                        trace.span,
                        action,
                        response
                    )

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

                if trace and trace.on_connection_create_end:
                    await trace.on_connection_create_end(
                        trace.span,
                        action,
                        response
                    )

                pipe.send_request_headers(action, stream)

                if trace and trace.on_request_headers_sent:
                    await trace.on_request_headers_sent(
                        trace.span,
                        action,
                        response
                    )
  
                if action.encoded_data is not None:
                    await pipe.submit_request_body(action, stream)

                response.write_end = time.monotonic()

                if action.encoded_data and trace and trace.on_request_data_sent:
                        await trace.on_request_data_sent(
                            trace.span,
                            action,
                            response
                        )

                await asyncio.wait_for(
                    pipe.receive_response(
                        action,
                        response, 
                        stream,
                        trace
                    ), 
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

                if trace and trace.on_request_exception:
                    await trace.on_request_exception(response)

            self.active -= 1
            if self.waiter and self.active <= self.pool.size:

                try:
                    self.waiter.set_result(None)
                    self.waiter = None

                except asyncio.InvalidStateError:
                    self.waiter = None

            if trace and trace.on_request_end:
                await trace.on_request_end(response)

            return response

    async def close(self):
        if self.closed is False:
            await self.pool.close()
            self.closed = True
