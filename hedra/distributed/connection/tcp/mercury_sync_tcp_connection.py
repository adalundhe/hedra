
import asyncio
import pickle
import socket
import ssl
import zstandard
from collections import deque, defaultdict
from hedra.distributed.connection.base.connection_type import ConnectionType
from hedra.distributed.encryption import AESGCMFernet
from hedra.distributed.env import Env
from hedra.distributed.env.time_parser import TimeParser
from hedra.distributed.models.base.message import Message
from hedra.distributed.snowflake.snowflake_generator import SnowflakeGenerator
from typing import (
    Tuple, 
    Deque, 
    Any, 
    Dict, 
    Coroutine, 
    AsyncIterable,
    Union,
    Optional
)
from hedra.distributed.connection.tcp.protocols import (
    MercurySyncTCPClientProtocol,
    MercurySyncTCPServerProtocol
)


class MercurySyncTCPConnection:

    def __init__(
        self,
        host: str,
        port: int,
        instance_id: int,
        env: Env
    ) -> None:

        self.id_generator = SnowflakeGenerator(instance_id)
        self.env = env

        self.host = host
        self.port = port

        self.events: Dict[
            str,
            Coroutine
        ] = {}

        self.queue: Dict[str, Deque[Tuple[str, int, float, Any]] ] = defaultdict(deque)
        self.parsers: Dict[str, Message] = {}
        self.connected = False
        self._running = False

        self._client_transports: Dict[str, asyncio.Transport] = {}
        self._server: asyncio.Server = None
        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self._waiters: Dict[str, Deque[asyncio.Future]] = defaultdict(deque)
        self._pending_responses: Deque[asyncio.Task]= deque()
        self._last_call: Deque[str] = deque()

        self._sent_values = deque()
        self.server_socket = None
        self._stream = False
        
        self._client_key_path: Union[str, None] = None
        self._client_cert_path: Union[str, None] = None

        self._server_key_path: Union[str, None] = None
        self._server_cert_path: Union[str, None] = None 
        
        self._client_ssl_context: Union[ssl.SSLContext, None] = None
        self._server_ssl_context: Union[ssl.SSLContext, None] = None
        
        self._encryptor = AESGCMFernet(env)
        self._semaphore: Union[asyncio.Semaphore, None] = None
        self._compressor: Union[zstandard.ZstdCompressor, None] = None
        self._decompressor: Union[zstandard.ZstdDecompressor, None] = None
        self._cleanup_task: Union[asyncio.Task, None] = None
        self._sleep_task: Union[asyncio.Task, None] = None
        self._cleanup_interval = TimeParser(env.MERCURY_SYNC_CLEANUP_INTERVAL).time

        self._request_timeout = TimeParser(
            env.MERCURY_SYNC_REQUEST_TIMEOUT
        ).time

        self._max_concurrency = env.MERCURY_SYNC_MAX_CONCURRENCY
        self._tcp_connect_retries = env.MERCURY_SYNC_TCP_CONNECT_RETRIES

        self.connection_type = ConnectionType.TCP

    def connect(
        self,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        worker_socket: Optional[socket.socket]=None
    ):

        try:

            self._loop = asyncio.get_event_loop()

        except Exception:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)
            
        self._running = True
        self._semaphore = asyncio.Semaphore(self._max_concurrency)

        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()

        if cert_path and key_path:
            self._server_ssl_context = self._create_server_ssl_context(
                cert_path=cert_path,
                key_path=key_path
            ) 

        if self.connected is False and worker_socket is None:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))

            self.server_socket.setblocking(False)

        elif self.connected is False:
            self.server_socket = worker_socket
            host, port = worker_socket.getsockname()
            
            self.host = host
            self.port = port

        if self.connected is False:

            server = self._loop.create_server(
                lambda: MercurySyncTCPServerProtocol(
                    self.read
                ),
                sock=self.server_socket,
                ssl=self._server_ssl_context
            )

            self._server = self._loop.run_until_complete(server)

            self.connected = True

            self._cleanup_task = self._loop.create_task(self._cleanup())

    async def connect_async(
        self,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        worker_socket: Optional[socket.socket]=None,
        worker_server: Optional[asyncio.Server]=None
    ):

        try:

            self._loop = asyncio.get_event_loop()

        except Exception:
            self._loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self._loop)

        self._running = True
        self._semaphore = asyncio.Semaphore(self._max_concurrency)

        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()

        if cert_path and key_path:
            self._server_ssl_context = self._create_server_ssl_context(
                cert_path=cert_path,
                key_path=key_path
            ) 

        if self.connected is False and worker_socket is None:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                self.server_socket.bind((self.host, self.port))

            except Exception:
                pass

            self.server_socket.setblocking(False)

        elif self.connected is False and worker_socket:
            self.server_socket = worker_socket
            host, port = worker_socket.getsockname()

            self.host = host
            self.port = port

        elif self.connected is False and worker_server:
            self._server = worker_server

            server_socket, _ = worker_server.sockets
            host, port = server_socket.getsockname()
            self.host = host
            self.port = port

            self.connected = True
            self._cleanup_task = self._loop.create_task(self._cleanup())

        if self.connected is False:

            server = await self._loop.create_server(
                lambda: MercurySyncTCPServerProtocol(
                    self.read
                ),
                sock=self.server_socket,
                ssl=self._server_ssl_context
            )

            self._server = server
            self.connected = True

            self._cleanup_task = self._loop.create_task(self._cleanup())

    def _create_server_ssl_context(
        self, 
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ) -> ssl.SSLContext:
        
        if self._server_cert_path is None:
            self._server_cert_path = cert_path

        if self._server_key_path is None:
            self._server_key_path = key_path

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_ctx.options |= ssl.OP_NO_TLSv1
        ssl_ctx.options |= ssl.OP_NO_TLSv1_1
        ssl_ctx.options |= ssl.OP_SINGLE_DH_USE
        ssl_ctx.options |= ssl.OP_SINGLE_ECDH_USE
        ssl_ctx.load_cert_chain(cert_path, keyfile=key_path)
        ssl_ctx.load_verify_locations(cafile=cert_path)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.VerifyMode.CERT_REQUIRED
        ssl_ctx.set_ciphers('ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384')

        return ssl_ctx

    async def connect_client(
        self,
        address: Tuple[str, int],
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        worker_socket: Optional[socket.socket]=None,
    ) -> None:
        
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_concurrency)
        
        self._loop = asyncio.get_event_loop()
        if cert_path and key_path:
            self._client_ssl_context = self._create_client_ssl_context(
                cert_path=cert_path,
                key_path=key_path
            ) 

        if worker_socket is None:

            tcp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            await self._loop.run_in_executor(None, tcp_socket.connect, address)

            tcp_socket.setblocking(False)

        else:

            tcp_socket = worker_socket

        last_error: Union[Exception, None] = None

        for _ in range(self._tcp_connect_retries):

            try:

                client_transport, _ = await self._loop.create_connection(
                    lambda: MercurySyncTCPClientProtocol(
                        self.read
                    ),
                    sock=tcp_socket,
                    ssl=self._client_ssl_context
                )

                self._client_transports[address] = client_transport

                return client_transport
            
            except ConnectionRefusedError as connection_error:
                last_error = connection_error

            await asyncio.sleep(1)

        if last_error:
            raise last_error
    
    def _create_client_ssl_context(
        self, 
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ) -> ssl.SSLContext:
        
        if self._client_cert_path is None:
            self._client_cert_path = cert_path

        if self._client_key_path is None:
            self._client_key_path = key_path

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.options |= ssl.OP_NO_TLSv1
        ssl_ctx.options |= ssl.OP_NO_TLSv1_1
        ssl_ctx.load_cert_chain(cert_path, keyfile=key_path)
        ssl_ctx.load_verify_locations(cafile=cert_path)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.VerifyMode.CERT_REQUIRED
        ssl_ctx.set_ciphers('ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384')

        return ssl_ctx
    
    async def _cleanup(self):
        while self._running:
            self._sleep_task = asyncio.create_task(
                asyncio.sleep(self._cleanup_interval)
            )

            await self._sleep_task

            for pending in list(self._pending_responses):
                if pending.done() or pending.cancelled():

                    try:
                        await pending

                    except (Exception, socket.error):
                        pass
                        # await self.close()
                        # await self.connect_async(
                        #     cert_path=self._client_cert_path,
                        #     key_path=self._client_key_path
                        # )

                    self._pending_responses.pop()

    async def send(
        self, 
        event_name: bytes,
        data: bytes, 
        address: Tuple[str, int]
    ) -> Tuple[int, Dict[str, Any]]:
        
        async with self._semaphore:

            try:
                self._last_call.append(event_name)

                client_transport = self._client_transports.get(address)
                if client_transport is None:
                    await self.connect_client(
                        address,
                        cert_path=self._client_cert_path,
                        key_path=self._client_key_path
                    )

                    client_transport = self._client_transports.get(address)

                item = pickle.dumps(
                    (
                        'request',
                        self.id_generator.generate(),
                        event_name,
                        data,
                        self.host,
                        self.port
                    ),
                    protocol=pickle.HIGHEST_PROTOCOL
                )

                encrypted_message = self._encryptor.encrypt(item)
                compressed = self._compressor.compress(encrypted_message)

                if client_transport.is_closing():
                    return (
                        self.id_generator.generate(),
                        Message(
                            host=self.host,
                            port=self.port,
                            error='Transport closed.'
                        )
                    )

                client_transport.write(compressed)

                waiter = self._loop.create_future()
                self._waiters[event_name].append(waiter)

                (
                    _,
                    shard_id,
                    _,
                    response_data,
                    _, 
                    _
                ) = await asyncio.wait_for(
                    waiter,
                    timeout=self._request_timeout
                )

                return (
                    shard_id,
                    response_data
                )
            
            except (Exception, socket.error):

                return (
                    self.id_generator.generate(),
                    Message(
                        host=self.host,
                        port=self.port,
                        error='Request timed out.'
                    )
                )
            
    async def send_bytes(
        self,
        event_name: str,
        data: bytes,
        address: Tuple[str, int]
    ) -> bytes:
        async with self._semaphore:

            try:
                self._last_call.append(event_name)

                client_transport = self._client_transports.get(address)
                if client_transport is None:
                    await self.connect_client(
                        address,
                        cert_path=self._client_cert_path,
                        key_path=self._client_key_path
                    )

                    client_transport = self._client_transports.get(address)

                if client_transport.is_closing():
                    return (
                        self.id_generator.generate(),
                        Message(
                            host=self.host,
                            port=self.port,
                            error='Transport closed.'
                        )
                    )

                client_transport.write(data)

                waiter = self._loop.create_future()
                self._waiters[event_name].append(waiter)
                
                return await asyncio.wait_for(
                    waiter,
                    timeout=self._request_timeout
                )
            
            except (Exception, socket.error):
                return b'Request timed out.'

    
    async def stream(
        self, 
        event_name: str,
        data: Any, 
        address: Tuple[str, int]
    ) -> AsyncIterable[Tuple[int, Dict[str, Any]]]: 
        
        async with self._semaphore:

            try:
                self._last_call.append(event_name)

                client_transport = self._client_transports.get(address)


                if self._stream is False:
                    item = pickle.dumps(
                        (
                            'stream_connect',
                            self.id_generator.generate(),
                            event_name,
                            data,
                            self.host,
                            self.port
                        ),
                        protocol=pickle.HIGHEST_PROTOCOL
                    )


                else:
                    item = pickle.dumps(
                        (
                            'stream',
                            self.id_generator.generate(),
                            event_name,
                            data,
                            self.host,
                            self.port
                        ),
                        protocol=pickle.HIGHEST_PROTOCOL
                    )

                encrypted_message = self._encryptor.encrypt(item)
                compressed = self._compressor.compress(encrypted_message)

                if client_transport.is_closing():
                    yield (
                        self.id_generator.generate(),
                        Message(
                            host=self.host,
                            port=self.port,
                            error='Transport closed.'
                        )
                    )

                client_transport.write(compressed)

                waiter = self._loop.create_future()
                self._waiters[event_name].append(waiter)

                await asyncio.wait_for(
                    waiter,
                    timeout=self._request_timeout
                )

                if self._stream is False:

                    self.queue[event_name].pop()

                    self._stream = True

                    item = pickle.dumps(
                        (
                            'stream',
                            self.id_generator.generate(),
                            event_name,
                            data,
                            self.host,
                            self.port
                        ),
                        pickle.HIGHEST_PROTOCOL
                    )

                    encrypted_message = self._encryptor.encrypt(item)
                    compressed = self._compressor.compress(encrypted_message)

                    client_transport.write(compressed)

                    waiter = self._loop.create_future()
                    self._waiters[event_name].append(waiter)

                    await waiter


                while bool(self.queue[event_name]) and self._stream:

                    (
                        _,
                        shard_id,
                        _,
                        response_data,
                        _, 
                        _
                    ) = self.queue[event_name].pop()
                
                    yield(
                        shard_id,
                        response_data
                    )
            
            except (Exception, socket.error):

                yield (
                    self.id_generator.generate(),
                    Message(
                        host=self.host,
                        port=self.port,
                        error='Request timed out.'
                    )
                )

        self.queue.clear()

    def read(
        self,
        data: bytes,
        transport: asyncio.Transport
    ) -> None:
        decompressed = b''

        try:
            decompressed = self._decompressor.decompress(data)

        except Exception as decompression_error:
            self._pending_responses.append(
                asyncio.create_task(
                    self._send_error(
                        error_message=str(decompression_error),
                        transport=transport
                    )
                )
            )

            if bool(self._last_call):
                event_name = self._last_call.pop()
                event_waiter = self._waiters[event_name]

                if bool(event_waiter):
                    waiter = event_waiter.pop()

                    try:

                        waiter.set_result(None)

                    except asyncio.InvalidStateError:
                        pass

            return

        decrypted = self._encryptor.decrypt(decompressed)

        result: Tuple[
            str, 
            int, 
            float, 
            Any, 
            str, 
            int
        ] = pickle.loads(decrypted)

        (
            message_type, 
            shard_id, 
            event_name,
            payload, 
            incoming_host, 
            incoming_port
        ) = result

        if message_type == 'request':
            self._pending_responses.append(
                asyncio.create_task(
                    self._read(
                        event_name,
                        self.events.get(event_name)(
                            shard_id,
                            self.parsers[event_name](**payload)
                        ),
                        transport
                    )
                )
            )

        elif message_type == "stream_connect":

            self.queue[event_name].append((
                message_type, 
                shard_id,
                event_name,
                payload, 
                incoming_host,
                incoming_port
            ))

            self._pending_responses.append(
                asyncio.create_task(
                    self._initialize_stream(
                        event_name,
                        transport
                    )
                )
            )

            event_waiter = self._waiters[event_name]

            if bool(event_waiter):
                waiter = event_waiter.pop()

                try:

                    waiter.set_result(None)

                except asyncio.InvalidStateError:
                    pass

        elif message_type == 'stream' or message_type == "stream_connect":

            self.queue[event_name].append((
                message_type, 
                shard_id,
                event_name,
                payload, 
                incoming_host,
                incoming_port
            ))

            self._pending_responses.append(
                asyncio.create_task(
                    self._read_iterator(
                        event_name,
                        self.events.get(event_name)(
                            shard_id,
                            self.parsers[event_name](**payload)
                        ),
                        transport
                    )
                )
            )

            event_waiter = self._waiters[event_name]

            if bool(event_waiter):
                waiter = event_waiter.pop()

                try:

                    waiter.set_result(None)

                except asyncio.InvalidStateError:
                    pass

        else:

            if event_name is None and bool(self._last_call):
                event_name = self._last_call.pop()

                
            event_waiter = self._waiters[event_name]

            if bool(event_waiter):
                waiter = event_waiter.pop()

                try:

                    waiter.set_result((
                        message_type, 
                        shard_id,
                        event_name,
                        payload, 
                        incoming_host,
                        incoming_port
                    ))

                except asyncio.InvalidStateError:
                    pass

    async def _read(
        self,
        event_name: str,
        coroutine: Coroutine,
        transport: asyncio.Transport
    ) -> Coroutine[Any, Any, None]:
        response: Message = await coroutine

        try:
            if transport.is_closing() is False:

                item = pickle.dumps(
                    (
                        'response', 
                        self.id_generator.generate(),
                        event_name,
                        response.to_data(), 
                        self.host,
                        self.port
                    ),
                    protocol=pickle.HIGHEST_PROTOCOL
                )

                encrypted_message = self._encryptor.encrypt(item)
                compressed = self._compressor.compress(encrypted_message)

                transport.write(compressed)

        except (Exception, socket.error):
            pass

    async def _read_iterator(
        self,
        event_name: str,
        coroutine: AsyncIterable[Message],
        transport: asyncio.Transport       
    ) -> Coroutine[Any, Any, None]:
        
        if transport.is_closing() is False:
  
            async for response in coroutine:
                
                try:

                    item = pickle.dumps(
                        (
                            'response', 
                            self.id_generator.generate(),
                            event_name,
                            response.to_data(), 
                            self.host,
                            self.port
                        ),
                        protocol=pickle.HIGHEST_PROTOCOL
                    )

                    encrypted_message = self._encryptor.encrypt(item)
                    compressed = self._compressor.compress(encrypted_message)

                    transport.write(compressed)

                except (Exception, socket.error):
                    pass

    async def _initialize_stream(
        self,
        event_name: str,
        transport: asyncio.Transport            
    ) -> Coroutine[Any, Any, None]:
        
        if transport.is_closing() is False:
        
            try:

                message = Message()
                item = pickle.dumps(
                    (
                        'response', 
                        self.id_generator.generate(),
                        event_name,
                        message.to_data(), 
                        self.host,
                        self.port
                    ),
                    protocol=pickle.HIGHEST_PROTOCOL
                )

                encrypted_message = self._encryptor.encrypt(item)
                compressed = self._compressor.compress(encrypted_message)

                transport.write(compressed)

            except (Exception, socket.error):
                pass

    async def _send_error(
        self,
        error_message: str,
        transport: asyncio.Transport
    ) -> Coroutine[Any, Any, None]:
        
        if transport.is_closing():
            
            try:
                
                error = Message(
                    error=error_message
                )

                item = pickle.dumps(
                    (
                        'response', 
                        self.id_generator.generate(),
                        None,
                        error.to_data(), 
                        self.host,
                        self.port
                    ),
                    protocol=pickle.HIGHEST_PROTOCOL
                )

                encrypted_message = self._encryptor.encrypt(item)
                compressed = self._compressor.compress(encrypted_message)

                transport.write(compressed)

            except (Exception, socket.error):
                pass
        
    async def close(self) -> None:
        self._stream = False
        self._running = False

        for client in self._client_transports.values():
            client.abort()
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            if self._cleanup_task.cancelled() is False:
                try:
                    self._sleep_task.cancel()
                    if not self._sleep_task.cancelled():
                        await self._sleep_task

                except (Exception, socket.error):
                    pass

                try:

                    await self._cleanup_task

                except Exception:
                    pass

            