import asyncio
import time
import uuid
from collections import deque
from typing import Dict, Any, Union, Coroutine, TypeVar, Optional
from hedra.core.engines.types.common.base_engine import BaseEngine
from hedra.core.engines.types.common.ssl import get_default_ssl_context
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.common.concurrency import Semaphore
from hedra.core.engines.types.common.protocols.udp.quic_protocol import (
    HeadersState,
    ResponseFrameCollection,
    FrameType,
    encode_frame
)
from hedra.core.engines.types.tracing.trace_session import (
    TraceSession, 
    Trace
)

from hedra.logging import HedraLogger
from hedra.versioning.flags.types.unstable.flag import unstable
from .connection import HTTP3Connection
from .action import HTTP3Action
from .result import HTTP3Result
from .pool import Pool


A = TypeVar('A')
R = TypeVar('R')


@unstable
class MercuryHTTP3Client(BaseEngine[Union[A, HTTP3Action], Union[R, HTTP3Result]]):

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
        'logger',
        'tracing_session'
    )

    def __init__(
        self, 
        concurrency: int=10**3, 
        timeouts: Timeouts = Timeouts(), 
        reset_connections: bool=False,
        tracing_session: Optional[TraceSession]=None
    ) -> None:
        super().__init__()

        self.session_id = str(uuid.uuid4())
        self.timeouts = timeouts

        self.registered: Dict[str, HTTP3Action] = {}
        self._hosts = {}
        self.closed = False

        self.sem = asyncio.Semaphore(value=concurrency)
        self.pool = Pool(concurrency, reset_connections=reset_connections)
        self.tracing_session: Union[TraceSession, None] = tracing_session
        self.logger = HedraLogger()
        self.logger.initialize()
        self.pool.create_pool()
        self.active = 0
        self.waiter = None

        self.ssl_context = get_default_ssl_context()

    async def set_pool(self, concurrency: int):
        self.sem = asyncio.Semaphore(value=concurrency)
        self.pool = Pool(concurrency, reset_connections=self.pool.reset_connections)
        self.pool.create_pool()

    def extend_pool(self, increased_capacity: int):
        self.pool.size += increased_capacity
        for _ in range(increased_capacity):
            self.pool.connections.append(
                HTTP3Connection(self.pool.reset_connections)
            )
        
        self.sem = Semaphore(self.pool.size)

    def shrink_pool(self, decrease_capacity: int):
        self.pool.size -= decrease_capacity
        self.pool.connections = self.pool.connections[:self.pool.size]
        self.sem = Semaphore(self.pool.size)
    
    async def prepare(self, action: HTTP3Action) -> Coroutine[Any, Any, None]:
        try:
                
            if self._hosts.get(action.url.hostname) is None:
                socket_configs = await asyncio.wait_for(action.url.lookup(), timeout=self.timeouts.connect_timeout)
            
                for ip_addr, configs in socket_configs.items():
                    for config in configs:

                        connection = HTTP3Connection()
                        
                        try:
                            await connection.make_connection(
                                ip_addr,
                                action.url.port,
                                config,
                                server_name=action.url.hostname,
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

    async def execute_prepared_request(self, action: HTTP3Action) -> Coroutine[Any, Any, HTTP3Result]:

        trace: Union[Trace, None] = None
        if self.tracing_session:
            trace = self.tracing_session.create_trace()
            await trace.on_request_start(action)
  
        response = HTTP3Result(action)
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
            
            try:

                connection = self.pool.connections.pop()
                
                if action.hooks.listen:
                    event = asyncio.Event()
                    action.hooks.channel_events.append(event)
                    await event.wait()

                if action.hooks.before:
                    action = await self.execute_before(action)
                    action.setup()

                response.start = time.monotonic()

                if trace and trace.on_connection_create_start:
                    await trace.on_connection_create_start(
                        trace.span,
                        action,
                        response
                    )

                await connection.make_connection(
                    action.url.ip_addr,
                    action.url.port,
                    action.url.socket_config,
                    server_name=action.url.hostname,
                    timeout=self.timeouts.connect_timeout
                )

                response.connect_end = time.monotonic()

                if trace and trace.on_connection_create_end:
                    await trace.on_connection_create_end(
                        trace.span,
                        action,
                        response
                    )

                stream_id = connection.protocol._quic.get_next_available_stream_id()
                
                stream = connection.protocol._get_or_create_stream(stream_id)
                if stream.headers_send_state == HeadersState.AFTER_TRAILERS:
                    raise Exception("HEADERS frame is not allowed in this state")

                encoder, frame_data = connection.protocol._encoder.encode(
                    stream_id, 
                    action.encoded_headers
                )

                connection.protocol._encoder_bytes_sent += len(encoder)
                connection.protocol._quic.send_stream_data(
                    connection.protocol._local_encoder_stream_id, 
                    encoder
                )

                # update state and send headers
                if stream.headers_send_state == HeadersState.INITIAL:
                    stream.headers_send_state = HeadersState.AFTER_HEADERS
                else:
                    stream.headers_send_state = HeadersState.AFTER_TRAILERS

                connection.protocol._quic.send_stream_data(
                    stream_id, encode_frame(
                        FrameType.HEADERS, 
                        frame_data
                    ), 
                    not action.encoded_data
                )

                if trace and trace.on_request_headers_sent:
                    await trace.on_request_headers_sent(
                        trace.span,
                        action,
                        response
                    )

                if action.encoded_data:
                    stream = connection.protocol._get_or_create_stream(stream_id)
                    if stream.headers_send_state != HeadersState.AFTER_HEADERS:
                        raise Exception("DATA frame is not allowed in this state")

                    connection.protocol._quic.send_stream_data(
                        stream_id, encode_frame(
                            FrameType.DATA, 
                            action.encoded_data
                        ), 
                        True
                    )
                    

                waiter = connection.protocol._loop.create_future()
                connection.protocol._request_events[stream_id] = deque()
                connection.protocol._request_waiter[stream_id] = waiter
                connection.protocol.transmit()

                response.write_end = time.monotonic()

                if action.encoded_data and trace and trace.on_request_data_sent:
                        await trace.on_request_data_sent(
                            trace.span,
                            action,
                            response
                        )

                response_frames: ResponseFrameCollection = await asyncio.wait_for(
                    waiter,
                    timeout=self.timeouts.total_timeout
                )

                headers: Dict[str, Union[bytes, int]] = {}
                for header_key, header_value in response_frames.headers_frame.headers:
                    headers[header_key] = header_value

                if trace and trace.on_response_headers_received:
                    await trace.on_response_headers_received(
                        trace.span,
                        action,
                        response
                    )
                
                response.headers = headers
                status = response.status

                if status >= 300 and status < 400:

                    if trace and trace.on_request_redirect:
                        await trace.on_request_redirect(
                            trace.span,
                            action,
                            response
                        )
                    
                    elapsed_time = 0
                    redirect_time_start = time.time()

                    for redirect_number in range(action.redirects):

                        if elapsed_time > self.timeouts.total_timeout:
                            response.status = 408
                            raise Exception('Request timed out while redirecting.')
                        
                        redirect_url = str(headers.get(b'location'))
                        if redirect_url.startswith('http') is False:
                            action.url.path = redirect_url
                            action.encoded_headers = None
                            action.setup()

                        else:
                            await action.url.replace(redirect_url)
                            action.encoded_headers = None
                            action.is_setup = False
                            await self.prepare(action)

                            await connection.make_connection(
                                action.url.ip_addr,
                                action.url.port,
                                action.url.socket_config,
                                server_name=action.url.hostname,
                                timeout=self.timeouts.connect_timeout
                            )

                        response.connect_end = time.monotonic()
                            
                        stream_id = connection.protocol._quic.get_next_available_stream_id()
                        
                        stream = connection.protocol._get_or_create_stream(stream_id)
                        if stream.headers_send_state == HeadersState.AFTER_TRAILERS:
                            raise Exception("HEADERS frame is not allowed in this state")

                        encoder, frame_data = connection.protocol._encoder.encode(
                            stream_id, 
                            action.encoded_headers
                        )

                        connection.protocol._encoder_bytes_sent += len(encoder)
                        connection.protocol._quic.send_stream_data(
                            connection.protocol._local_encoder_stream_id, 
                            encoder
                        )

                        # update state and send headers
                        if stream.headers_send_state == HeadersState.INITIAL:
                            stream.headers_send_state = HeadersState.AFTER_HEADERS
                        else:
                            stream.headers_send_state = HeadersState.AFTER_TRAILERS
                        connection.protocol._quic.send_stream_data(
                            stream_id, encode_frame(
                                FrameType.HEADERS, 
                                frame_data
                            ), 
                            not action.encoded_data
                        )

                        if action.encoded_data:
                            stream = connection.protocol._get_or_create_stream(stream_id)
                            if stream.headers_send_state != HeadersState.AFTER_HEADERS:
                                raise Exception("DATA frame is not allowed in this state")

                            connection.protocol._quic.send_stream_data(
                                stream_id, encode_frame(
                                    FrameType.DATA, 
                                    action.encoded_data
                                ), 
                                True
                            )
                            

                        waiter = connection.protocol._loop.create_future()
                        connection.protocol._request_events[stream_id] = deque()
                        connection.protocol._request_waiter[stream_id] = waiter
                        connection.protocol.transmit()

                        response.write_end = time.monotonic()

                        response_frames: ResponseFrameCollection = await asyncio.wait_for(
                            waiter,
                            timeout=self.timeouts.total_timeout
                        )

                        for header_key, header_value in response_frames.headers_frame.headers:
                            headers[header_key] = header_value

                        response.headers = headers
                        status = response.status
                        if status >= 200 and status < 300:
                            break

                        action.redirects -= redirect_number
                        elapsed_time = time.time() - redirect_time_start

                response.complete = time.monotonic()

                if trace and trace.on_response_data_received:
                    await trace.on_response_data_received(
                        trace.span,
                        action,
                        response
                    )

                response.headers = headers
                self.pool.connections.append(connection)

                if action.hooks.after:
                    response = await self.execute_after(action, response)
                    action.setup()

                if action.hooks.checks:
                    response = await self.execute_checks(action, response)

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

            except Exception as e:
                response.complete = time.monotonic()
                response.error = str(e)

                self.pool.connections.append(HTTP3Connection(reset_connection=self.pool.reset_connections))

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
