import time
import uuid
import asyncio
from typing import (
    Coroutine, 
    Any, 
    Optional, 
    Union
)
from hedra.core.engines.types.http import MercuryHTTPClient
from hedra.core.engines.types.http.connection import HTTPConnection
from hedra.core.engines.types.common import Timeouts
from hedra.core.engines.types.tracing.trace_session import (
    TraceSession, 
    Trace
)
from .action import GraphQLAction
from .result import GraphQLResult


class MercuryGraphQLClient(MercuryHTTPClient[GraphQLAction, GraphQLResult]):

    def __init__(
        self, 
        concurrency: int = 10 ** 3, 
        timeouts: Timeouts = Timeouts(), 
        reset_connections: bool = False,
        tracing_session: Optional[TraceSession]=None
    ) -> None:

        super(
            MercuryGraphQLClient,
            self
        ).__init__(
            concurrency=concurrency, 
            timeouts=timeouts, 
            reset_connections=reset_connections,
            tracing_session=tracing_session
        )

        self.session_id = str(uuid.uuid4())

    async def execute_prepared_request(self, action: GraphQLAction) -> Coroutine[Any, Any, GraphQLResult]:

        trace: Union[Trace, None] = None
        if self.tracing_session:
            trace = self.tracing_session.create_trace()
            await trace.on_request_start(action)
  
        response = GraphQLResult(action)
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
                    action.url.hostname,
                    action.url.ip_addr,
                    action.url.port,
                    action.url.socket_config,
                    timeout=self.timeouts.connect_timeout,
                    ssl=action.ssl_context
                )

                response.connect_end = time.monotonic()

                if trace and trace.on_connection_create_end:
                    await trace.on_connection_create_end(
                        trace.span,
                        action,
                        response
                    )

                connection.write(action.encoded_headers)

                if trace and trace.on_request_headers_sent:
                    await trace.on_request_headers_sent(
                        trace.span,
                        action,
                        response
                    )
                
                if action.encoded_data:
                    if action.is_stream:
                        action.write_chunks(
                            connection,
                            trace
                        )

                    else:
                        connection.write(action.encoded_data)

                response.write_end = time.monotonic()

                if action.encoded_data and trace and trace.on_request_data_sent:
                        await trace.on_request_data_sent(
                            trace.span,
                            action,
                            response
                        )

                response.response_code = await asyncio.wait_for(
                    connection.reader.readline_fast(),
                    timeout=self.timeouts.socket_read_timeout
                )

                headers = await asyncio.wait_for(
                    connection.read_headers(),
                    timeout=self.timeouts.socket_read_timeout
                )

                if trace and trace.on_response_headers_received:
                    await trace.on_response_headers_received(
                        trace.span,
                        action,
                        response
                    )

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

                    for _ in range(action.redirects):

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

                        response.response_code = await asyncio.wait_for(
                            connection.reader.readline_fast(),
                            timeout=self.timeouts.socket_read_timeout
                        )

                        headers = await asyncio.wait_for(
                            connection.read_headers(),
                            timeout=self.timeouts.socket_read_timeout
                        )

                        status = response.status

                        if status >= 200 and status < 300:
                            break

                        elapsed_time = time.time() - redirect_time_start

                content_length = headers.get(b'content-length')
                transfer_encoding = headers.get(b'transfer-encoding')

                # We require Content-Length or Transfer-Encoding headers to read a
                # request body, otherwise it's anyone's guess as to how big the body
                # is, and we ain't playing that game.
                body = bytearray()
                if content_length:
                    body = await asyncio.wait_for(
                        connection.readexactly(int(content_length)),
                        timeout=self.timeouts.socket_read_timeout
                    )

                elif transfer_encoding:
                    
                    all_chunks_read = False

                    while True and not all_chunks_read:

                        chunk_size = int((await connection.readuntil()).rstrip(), 16)
    
                        if not chunk_size:
                            # read last CRLF
                            body.extend(
                                await asyncio.wait_for(
                                    connection.readuntil(),
                                    timeout=self.timeouts.socket_read_timeout
                                )
                            )
                            
                            break
                        
                        chunk = await asyncio.wait_for(
                            connection.readexactly(chunk_size + 2),
                            timeout=self.timeouts.socket_read_timeout
                        )

                        body.extend(
                            chunk[:-2]
                        )

                        if trace and trace.on_response_chunk_received:
                            await trace.on_response_chunk_received(
                                trace.span,
                                action,
                                response
                            )

                    all_chunks_read = True
         
                response.complete = time.monotonic()

                if trace and trace.on_response_data_received:
                    await trace.on_response_data_received(
                        trace.span,
                        action,
                        response
                    )

                response.headers = headers
                response.body = body
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

                self.pool.connections.append(HTTPConnection(reset_connection=self.pool.reset_connections))

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