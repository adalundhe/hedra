import time
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

    async def execute_prepared_request(self, action: GraphQLAction) -> GraphQLResponseFuture:
   
        response = GraphQLResult(action)
        response.wait_start = time.monotonic()

        async with self.sem:
            connection = self.pool.connections.pop()
            
            try:
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
                    action = await action.hooks.after(action, response)
                    action.setup()

            except Exception as e:
                response.read_end = time.monotonic()
                response.error = str(e)
                await connection.close()
                
                self.pool.connections.append(HTTPConnection(reset_connection=self.pool.reset_connections))

            self.active -= 1
            if self.waiter and self.active <= self.pool.size:

                try:
                    self.waiter.set_result(None)
                    self.waiter = None

                except asyncio.InvalidStateError:
                    self.waiter = None

            return response