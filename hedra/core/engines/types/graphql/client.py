import time
import uuid
import asyncio
from hedra.core.engines.types.http import MercuryHTTPClient
from hedra.core.engines.types.http.connection import HTTPConnection
from hedra.core.engines.types.common import Timeouts
from typing import Awaitable, Union
from .action import GraphQLAction
from .result import GraphQLResult


GraphQLResponseFuture = Awaitable[Union[GraphQLAction, Exception]]


class MercuryGraphQLClient(MercuryHTTPClient):

    def __init__(
        self, 
        concurrency: int = 10 ** 3, 
        timeouts: Timeouts = Timeouts(), 
        reset_connections: bool = False
    ) -> None:

        super(
            MercuryGraphQLClient,
            self
        ).__init__(
            concurrency, 
            timeouts, 
            reset_connections
        )

        self.session_id = str(uuid.uuid4())

    async def execute_prepared_request(self, action: GraphQLAction) -> GraphQLResponseFuture:
   
        response = GraphQLResult(action)
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

                        redirect_action = GraphQLAction(
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