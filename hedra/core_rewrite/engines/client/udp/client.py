import asyncio
import ssl
import time
from collections import defaultdict
from typing import Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import urlparse

from pydantic import BaseModel

from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core_rewrite.engines.client.shared.models import (
    URL,
    URLMetadata,
)

from .models.udp import UDPRequest, UDPResponse
from .protocols import UDPConnection


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
    
    async def send(
        self,
        url: str,
        data: str | bytes | BaseModel,
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,   
    ):
        async with self._semaphore:
            try:
                return await asyncio.wait_for(
                    self._request(
                        UDPRequest(
                            url=url,
                            method='SEND',
                            data=data,
                        )
                    ),
                    timeout=timeout
                )
            
            except Exception as err:
                url_data = urlparse(url)

                return UDPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path
                    ),
                    error=str(err)
                )
            
    async def receive(
        self,
        url: str,
        delimiter: Optional[str | bytes]=b'\n',
        response_size: Optional[int]=None,
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,   
    ):
        async with self._semaphore:
            try:
                if isinstance(delimiter, str):
                    delimiter = delimiter.encode()

                return await asyncio.wait_for(
                    self._request(
                        UDPRequest(
                            url=url,
                            method='RECEIVE',
                            response_size=response_size,
                            delimiter=delimiter
                        )
                    ),
                    timeout=timeout
                )
            
            except Exception as err:
                url_data = urlparse(url)

                return UDPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path
                    ),
                    error=str(err)
                )
            
    async def bidirectional(
        self,
        url: str,
        data: str | bytes | BaseModel,
        delimiter: Optional[str | bytes]=b'\n',
        response_size: Optional[int]=None,
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,  
    ):
        async with self._semaphore:
            try:
                if isinstance(delimiter, str):
                    delimiter = delimiter.encode()

                return await asyncio.wait_for(
                    self._request(
                        UDPRequest(
                            url=url,
                            method='BIDIRECTIONAL',
                            data=data,
                            response_size=response_size,
                            delimiter=delimiter
                        )
                    ),
                    timeout=timeout
                )
            
            except Exception as err:
                url_data = urlparse(url)

                return UDPResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path
                    ),
                    error=str(err)
                )

    async def _request(
        self,
        request: UDPRequest,
        timings: Dict[
            Literal[
                'request_start',
                'connect_start',
                'connect_end',
                'write_start',
                'write_end',
                'read_start',
                'read_end',
                'request_end'
            ],
            float | None
        ]={}
    ):
        request_url = request.url

        if timings['connect_start'] is None:
            timings['connect_start'] = time.monotonic()
        
        (
            error,
            connection, 
            url, 
        ) = await asyncio.wait_for(
            self._connect_to_url_location(request_url),
            timeout=self.timeouts.connect_timeout
        )

        if connection.reader is None:
            timings['connect_end'] = time.monotonic()
            self._connections.append(
                UDPConnection(
                    reset_connections=self.reset_connections
                )
            )

            return UDPResponse(
                url=URLMetadata(
                    host=url.hostname,
                    path=url.path
                ),
                error=str(error),
                timings=timings
            ), False, timings
        
        timings['connect_end'] = time.monotonic()

        data = request.prepare()
        response_data = b''

        try:
            match request.method:
                case 'BIDIRECTIONAL':

                    timings['write_start'] = time.monotonic()
    
                    connection.writer.write(data)
                    
                    timings['write_end'] = time.monotonic()
                    timings['read_start'] = time.monotonic()

                    if request.response_size:
                        response_data = await connection.reader.readexactly(request.response_size)

                    else:
                        response_data = await connection.reader.readuntil(separator=request.delimiter)
                    
                    timings['read_end'] = time.monotonic()

                case 'SEND':
                    timings['write_start'] = time.monotonic()
                    connection.writer.write(data)
                    timings['write_end'] = time.monotonic()

                case 'RECEIVE':
                    timings['read_start'] = time.monotonic()

                    if request.response_size:
                        response_data = await connection.reader.readexactly(request.response_size)

                    else:
                        response_data = await connection.reader.readuntil(separator=request.delimiter)

                    timings['read_end'] = time.monotonic()

                case _:

                    if timings['write_start']:
                        timings['write_end'] = time.monotonic()

                    if timings['read_start']:
                        timings['read_end'] = time.monotonic()

                    raise Exception('Err. - invalid UDP operation. Must be one of - BIDIRECTIONAL, SEND, or RECEIVE.')
                
            self._connections.append(connection)

            return UDPResponse(
                url=URLMetadata(
                    host=url.hostname,
                    path=url.path
                ),
                content=response_data,
                timings=timings
            )
                
        except Exception as err:
            self._connections.append(
                UDPConnection(reset_connections=self.reset_connections)
            )

            return UDPResponse(
                url=URLMetadata(
                    host=url.hostname,
                    path=url.path
                ),
                error=str(err),
                timings=timings
            ), False, timings
                
            

    async def _connect_to_url_location(
        self,
        request_url: str,
    ) -> Tuple[
        Optional[Exception],
        UDPConnection,
        URL,
    ]:
        
        parsed_url = URL(request_url)

        url = self._url_cache.get(parsed_url.hostname)
        dns_lock = self._dns_lock[parsed_url.hostname]
        dns_waiter = self._dns_waiters[parsed_url.hostname]

        do_dns_lookup = url is None
        
        if do_dns_lookup and dns_lock.locked() is False:
            await dns_lock.acquire()
            url = parsed_url
            await url.lookup()

            self._dns_lock[parsed_url.hostname] = dns_lock
            self._url_cache[parsed_url.hostname] = url
            
            dns_waiter = self._dns_waiters[parsed_url.hostname]

            if dns_waiter.done() is False:
                dns_waiter.set_result(None)

            dns_lock.release()

        elif do_dns_lookup:
            await dns_waiter
            url = self._url_cache.get(parsed_url.hostname)

        connection_error: Optional[Exception] = None
        connection = self._connections.pop()

        if url.address is None:
            for address, ip_info in url:

                try:
                    
                    await connection.make_connection(
                        url.address,
                        url.port,
                        url.socket_config,
                        tls=self._udp_ssl_context if 'wss' in url.scheme else None
                    )

                    url.address = address
                    url.socket_config = ip_info

                except Exception:
                    pass
                    
        else:
            try:
                    
                await connection.make_connection(
                    url.address,
                    url.port,
                    url.socket_config,
                    tls=self._udp_ssl_context if 'wss' in url.scheme else None
                )

            except Exception as connection_error:
                
                raise connection_error

        return connection_error, connection, parsed_url