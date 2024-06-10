import asyncio
import ssl
import time
from collections import defaultdict
from typing import (
    Any,
    Coroutine,
    Dict,
    List,
    Optional,
    Tuple,
)

from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core_rewrite.engines.client.shared.models import (
    URL,
)

from .action import UDPAction
from .connection import UDPConnection
from .result import UDPResult


class MercuryUDPClient:


    def __init__(
        self,
        pool_size: Optional[int]=None,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        reset_connections: bool =False
    ) -> None:
   
        self.timeouts = Timeouts()
        self.reset_connections = reset_connections
 
        self._cert_path = cert_path
        self._key_path = key_path
        self._udp_ssl_context = self._create_udp_ssl_context(
                cert_path=cert_path,
                key_path=key_path,
        )
        
        self._dns_lock: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: List[asyncio.Future] = []

        self._client_waiters: Dict[asyncio.Transport, asyncio.Future] = {}
        self._connections: List[UDPConnection] = [
            UDPConnection(
                reset_connection=reset_connections
            ) for _ in range(pool_size)
        ]

        self._hosts: Dict[str, Tuple[str, int]] = {}

        self._connections_count: Dict[str, List[asyncio.Transport]] = defaultdict(list)
        self._locks: Dict[asyncio.Transport, asyncio.Lock] = {}

        self._max_concurrency = pool_size

        self._semaphore = asyncio.Semaphore(self._max_concurrency)
        self._connection_waiters: List[asyncio.Future] = []

        self._url_cache: Dict[
            str,
            URL
        ] = {}


    def _create_udp_ssl_context(
        self,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ) -> ssl.SSLContext: 
        
        if self._udp_cert_path is None:
            self._udp_cert_path = cert_path

        if self._udp_key_path is None:
            self._udp_key_path = key_path

        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS)
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

    async def prepare(self, action: UDPAction) -> Coroutine[Any, Any, None]:
        try:
            if action.url.is_ssl:
                action.ssl_context = self.ssl_context

            if self._hosts.get(action.url.hostname) is None:

                    socket_configs = await asyncio.wait_for(action.url.lookup(), timeout=self.timeouts.connect_timeout)
              
                    for ip_addr, configs in socket_configs.items():
                        for config in configs:

                            connection = UDPConnection()
                            
                            try:
                                await connection.make_connection(
                                    ip_addr,
                                    action.url.port,
                                    config,
                                    timeout=self.timeouts.connect_timeout
                                )

                                action.url.socket_config = config
                                action.url.ip_addr = ip_addr
                                action.url.has_ip_addr = True
                                break

                            except Exception: 
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
                UDPConnection(self.pool.reset_connections)
            )
        
        self.sem = asyncio.Semaphore(self.pool.size)

    def shrink_pool(self, decrease_capacity: int):
        self.pool.size -= decrease_capacity
        self.pool.connections = self.pool.connections[:self.pool.size]
        self.sem = asyncio.Semaphore(self.pool.size)

    async def execute_prepared_request(self, action: UDPAction) -> Coroutine[Any, Any, UDPResult]:
 
        response = UDPResult(action)
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
                    action = await self.execute_before(action)
                    action.setup()

                response.start = time.monotonic()

                await connection.make_connection(
                    action.url.ip_addr,
                    action.url.port,
                    action.url.socket_config,
                    timeout=self.timeouts.connect_timeout
                )

                response.connect_end = time.monotonic()
                
                if action.encoded_data:
                    if action.is_stream:
                        action.write_chunks(connection)

                    else:
                        connection.write(action.encoded_data)

                response.write_end = time.monotonic()
                
                if action.wait_for_response:
                    response.body = await connection.readuntil()
         
                response.complete = time.monotonic()

                self.pool.connections.append(connection)

                if action.hooks.after:
                    response = await self.execute_after(action, response)
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

            except Exception as e:
                response.complete = time.monotonic()
                response.error = str(e)

                self.pool.connections.append(UDPConnection(reset_connection=self.pool.reset_connections))

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
