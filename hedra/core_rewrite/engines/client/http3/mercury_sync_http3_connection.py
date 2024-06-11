import asyncio
import ssl
import time
from collections import defaultdict, deque
from typing import (
    Dict,
    List,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
from urllib.parse import urlparse

from pydantic import BaseModel

from hedra.core_rewrite.engines.client.http3.protocols.quic_protocol import (
    FrameType,
    HeadersState,
    ResponseFrameCollection,
    encode_frame,
)
from hedra.core_rewrite.engines.client.shared.models import (
    URL,
    Cookies,
    HTTPCookie,
    HTTPEncodableValue,
    URLMetadata,
)
from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts
from hedra.versioning.flags.types.unstable.flag import unstable

from .models import HTTP3Request, HTTP3Response
from .protocols import HTTP3Connection

A = TypeVar('A')
R = TypeVar('R')


@unstable
class MercurySyncHTTP3Connection:

    def __init__(
        self, 
        pool_size: Optional[int]=None,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        reset_connections: bool =False
    ) -> None:
        
        if pool_size is None:
            pool_size = 100
        
        self.timeouts = Timeouts()
        self.reset_connections = reset_connections
 
        self._cert_path = cert_path
        self._key_path = key_path
        self._client_ssl_context = self._create_general_client_ssl_context()
        
        self._dns_lock: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: List[asyncio.Future] = []

        self._client_waiters: Dict[asyncio.Transport, asyncio.Future] = {}
        self._connections: List[HTTP3Connection] = [
            HTTP3Connection(
                reset_connections=reset_connections
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

    def _create_general_client_ssl_context(self):
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        return ctx
    
    async def head(
        self,
        url: str,
        auth: Optional[Tuple[str, str]]=None,
        cookies: Optional[List[HTTPCookie]]=None,
        headers: Dict[str, str]={},
        params: Optional[Dict[str, HTTPEncodableValue]]=None,
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,
        
        
        redirects: int=3
    ):
        async with self._semaphore:

            try:
                
                return await asyncio.wait_for(
                    self._request(
                        HTTP3Request(
                            url=url,
                            method='HEAD',
                            cookies=cookies,
                            auth=auth,
                            headers=headers,
                            params=params,
                            redirects=redirects
                        ),
                    ),
                    timeout=timeout
                )
            
            except asyncio.TimeoutError:

                url_data = urlparse(url)

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query
                    ),
                    headers=headers,
                    method='HEAD',
                    status=408,
                    status_message='Request timed out.'
                )
            
    async def options(
        self,
        url: str,
        auth: Optional[Tuple[str, str]]=None,
        cookies: Optional[List[HTTPCookie]]=None,
        headers: Dict[str, str]={},
        params: Optional[Dict[str, HTTPEncodableValue]]=None,
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,   
        redirects: int=3
    ):
        async with self._semaphore:

            try:
                
                return await asyncio.wait_for(
                    self._request(
                        HTTP3Request(
                            url=url,
                            method='OPTIONS',
                            cookies=cookies,
                            auth=auth,
                            headers=headers,
                            params=params,
                            redirects=redirects
                        ),
                    ),
                    timeout=timeout
                )
            
            except asyncio.TimeoutError:

                url_data = urlparse(url)

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query
                    ),
                    headers=headers,
                    method='OPTIONS',
                    status=408,
                    status_message='Request timed out.'
                )
    
    async def get(
        self,
        url: str,
        auth: Optional[Tuple[str, str]]=None,
        cookies: Optional[List[HTTPCookie]]=None,
        headers: Dict[str, str]={},
        params: Optional[Dict[str, HTTPEncodableValue]]=None,
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,
        redirects: int=3
    ):
        
        async with self._semaphore:
            
            try:
            
                return await asyncio.wait_for(
                    self._request(
                        HTTP3Request(
                            url=url,
                            method='GET',
                            cookies=cookies,
                            data=None,
                            auth=auth,
                            headers=headers,
                            params=params,
                            redirects=redirects
                        )
                    ),
                    timeout=timeout
                )
            
            except asyncio.TimeoutError:

                url_data = urlparse(url)

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query
                    ),
                    headers=headers,
                    method='GET',
                    status=408,
                    status_message='Request timed out.'
                )
        
    async def post(
        self,
        url: str,
        auth: Optional[Tuple[str, str]]=None,
        cookies: Optional[List[HTTPCookie]]=None,
        headers: Dict[str, str]={},
        params: Optional[Dict[str, HTTPEncodableValue]]=None,
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,
        data: Union[
            Optional[str],
            Optional[BaseModel]
        ]=None,
        
        
        redirects: int=3
    ):
        async with self._semaphore:

            try:
                
                return await asyncio.wait_for(
                    self._request(
                        HTTP3Request(
                            url=url,
                            method='POST',
                            cookies=cookies,
                            auth=auth,
                            headers=headers,
                            params=params,
                            data=data,
                            redirects=redirects
                        ),
                    ),
                    timeout=timeout
                )
            
            except asyncio.TimeoutError:

                url_data = urlparse(url)

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query
                    ),
                    headers=headers,
                    method='POST',
                    status=408,
                    status_message='Request timed out.'
                )
        
    async def put(
        self,
        url: str,
        auth: Optional[Tuple[str, str]]=None,
        cookies: Optional[List[HTTPCookie]]=None,
        headers: Dict[str, str]={},
        params: Optional[Dict[str, HTTPEncodableValue]]=None,
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,
        data: Union[
            Optional[str],
            Optional[BaseModel]
        ]=None,
        redirects: int=3
    ):
        async with self._semaphore:

            try:
                
                return await asyncio.wait_for(
                    self._request(
                        HTTP3Request(
                            url=url,
                            method='PUT',
                            cookies=cookies,
                            auth=auth,
                            headers=headers,
                            params=params,
                            data=data,
                            redirects=redirects
                        ),
                    ),
                    timeout=timeout
                )
            
            except asyncio.TimeoutError:

                url_data = urlparse(url)

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query
                    ),
                    headers=headers,
                    method='PUT',
                    status=408,
                    status_message='Request timed out.'
                )
    
    async def patch(
        self,
        url: str,
        auth: Optional[Tuple[str, str]]=None,
        cookies: Optional[List[HTTPCookie]]=None,
        headers: Dict[str, str]={},
        params: Optional[Dict[str, HTTPEncodableValue]]=None,
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,
        data: Union[
            Optional[str],
            Optional[BaseModel]
        ]=None,
        
        
        redirects: int=3
    ):
        async with self._semaphore:
                
            try:
                
                return await asyncio.wait_for(
                    self._request(
                        HTTP3Request(
                            url=url,
                            method='PATCH',
                            cookies=cookies,
                            auth=auth,
                            headers=headers,
                            params=params,
                            data=data,
                            redirects=redirects
                        ),
                    ),
                    timeout=timeout
                )
            
            except asyncio.TimeoutError:

                url_data = urlparse(url)

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query
                    ),
                    headers=headers,
                    method='PATCH',
                    status=408,
                    status_message='Request timed out.'
                )
        
    async def delete(
        self,
        url: str,
        auth: Optional[Tuple[str, str]]=None,
        cookies: Optional[List[HTTPCookie]]=None,
        headers: Dict[str, str]={},
        params: Optional[Dict[str, HTTPEncodableValue]]=None,
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,
        
        
        redirects: int=3
    ):
        
        async with self._semaphore:
        
            try:
                
                return await asyncio.wait_for(
                    self._request(
                        HTTP3Request(
                            url=url,
                            method='DELETE',
                            cookies=cookies,
                            auth=auth,
                            headers=headers,
                            params=params,
                            redirects=redirects
                        ),
                    ),
                    timeout=timeout
                )
            
            except asyncio.TimeoutError:

                url_data = urlparse(url)

                return HTTP3Response(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query
                    ),
                    headers=headers,
                    method='DELETE',
                    status=408,
                    status_message='Request timed out.'
                )
    
    async def _request(
        self, 
        request: HTTP3Request, 
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ):
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
        ] = {
            'request_start': None,
            'connect_start': None,
            'connect_end': None,
            'write_start': None,
            'write_end': None,
            'read_start': None,
            'read_end': None,
            'request_end': None
        }
        timings['request_start'] = time.monotonic()

        if cert_path is None:
            cert_path = self._cert_path

        if key_path is None:
            key_path = self._key_path
        

        result, redirect, timings = await self._execute(
            request,
            timings=timings
        )

        if redirect:

            location = result.headers.get('location')

            upgrade_ssl = False
            if 'https' in location and 'https' not in request.url:
                upgrade_ssl = True

            for _ in range(request.redirects):

                result, redirect, timings = await self._execute(
                    request,
                    upgrade_ssl=upgrade_ssl,
                    redirect_url=location,
                    timings=timings
                )

                if redirect is False:
                    break

                location = result.headers.get('location')

                upgrade_ssl = False
                if 'https' in location and 'https' not in request.url:
                    upgrade_ssl = True

        timings['request_end'] = time.monotonic()
        result.timings.update(timings)

        return result
        
    async def _execute(
        self,
        request: HTTP3Request,
        upgrade_ssl: bool=False,
        redirect_url: Optional[str]=None,
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
    ) -> Tuple[
        HTTP3Response, 
        bool,
        Dict[
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
        ]
    ]:
    
        if redirect_url:
            request_url = redirect_url

        else:
            request_url = request.url

        try:

            if timings['connect_start'] is None:
                timings['connect_start'] = time.monotonic()
            
            (
                connection, 
                url, 
                upgrade_ssl
            ) = await asyncio.wait_for(
                self._connect_to_url_location(
                    request_url,
                    ssl_redirect_url=request_url if upgrade_ssl else None
                ),
                timeout=self.timeouts.connect_timeout
            )

            if upgrade_ssl:

                ssl_redirect_url = request_url.replace('http://', 'https://')

                connection, url, _ = await asyncio.wait_for(
                    self._connect_to_url_location(
                        request_url,
                        ssl_redirect_url=ssl_redirect_url
                    ),
                    timeout=self.timeouts.connect_timeout
                )

                request_url = ssl_redirect_url

            encoded_headers, data = request.prepare(url)

            if connection.protocol is None:
                
                timings['connect_end'] = time.monotonic()
                self._connections.append(
                    HTTP3Connection(
                        reset_connections=self.reset_connections,
                    )
                )

                return HTTP3Response(
                    url=URLMetadata(
                        host=url.hostname,
                        path=url.path
                    ),
                    method=request.method,
                    status=400,
                    headers=request.headers,
                    timings=timings,
                ), False, timings
            

            timings['connect_end'] = time.monotonic()

            if timings['write_start'] is None:
                timings['write_start'] = time.monotonic()

            stream_id = connection.protocol.quic.get_next_available_stream_id()
                
            stream = connection.protocol.get_or_create_stream(stream_id)
            if stream.headers_send_state == HeadersState.AFTER_TRAILERS:
                raise Exception("HEADERS frame is not allowed in this state")

            encoder, frame_data = connection.protocol.encoder.encode(
                stream_id, 
                encoded_headers
            )

            connection.protocol.encoder_bytes_sent += len(encoder)
            connection.protocol.quic.send_stream_data(
                connection.protocol._local_encoder_stream_id, 
                encoder
            )

            # update state and send headers
            if stream.headers_send_state == HeadersState.INITIAL:
                stream.headers_send_state = HeadersState.AFTER_HEADERS
            else:
                stream.headers_send_state = HeadersState.AFTER_TRAILERS

            connection.protocol.quic.send_stream_data(
                stream_id, 
                encode_frame(
                    FrameType.HEADERS, 
                    frame_data
                ), 
                not data,
            )

            if data:
                stream = connection.protocol.get_or_create_stream(stream_id)
                if stream.headers_send_state != HeadersState.AFTER_HEADERS:
                    raise Exception("DATA frame is not allowed in this state")

                connection.protocol.quic.send_stream_data(
                    stream_id, encode_frame(
                        FrameType.DATA, 
                        data
                    ), 
                    True
                )

            waiter = connection.protocol.loop.create_future()
            connection.protocol.request_events[stream_id] = deque()
            connection.protocol._request_waiter[stream_id] = waiter
            connection.protocol.transmit()

            if timings['write_end'] is None:
                timings['write_end'] = time.monotonic()

            if timings['read_start'] is None:
                timings['read_start'] = time.monotonic()

            response_frames: ResponseFrameCollection = await asyncio.wait_for(
                waiter,
                timeout=self.timeouts.total_timeout
            )

            headers: Dict[str, Union[bytes, int]] = {}
            for header_key, header_value in response_frames.headers_frame.headers:
                headers[header_key] = header_value
 
            status = int(headers.get(b':status', b'400'))
        
            if status >= 300 and status < 400:
                timings['read_end'] = time.monotonic()
                self._connections.append(connection)

                return HTTP3Response(
                    url=URLMetadata(
                        host=url.hostname,
                        path=url.path,
                        params=url.params,
                        query=url.query,
                    ),
                    method=request.method,
                    status=status,
                    headers=headers,
                    timings=timings,
                ), True, timings

            cookies: Union[Cookies, None] = None
            cookies_data: Union[bytes, None] = headers.get(b'set-cookie')
            if cookies_data:
                cookies = Cookies()
                cookies.update(cookies_data)

            self._connections.append(connection)

            timings['read_end'] = time.monotonic()

            return HTTP3Response(
                url=URLMetadata(
                    host=url.hostname,
                    path=url.path,
                    params=url.params,
                    query=url.query,
                ),
                cookies=cookies,
                method=request.method,
                status=status,
                headers=headers,
                content=response_frames.body,
                timings=timings,
            ), False, timings

        except Exception as request_exception:
            self._connections.append(
                HTTP3Connection(
                    reset_connections=self.reset_connections
                )
            )

            if isinstance(request_url, str):
                request_url = urlparse(request_url)

            timings['read_end'] = time.monotonic()

            return HTTP3Response(
                url=URLMetadata(
                    host=request_url.hostname,
                    path=request_url.path,
                    params=request_url.params,
                    query=request_url.query,
                ),
                method=request.method,
                status=400,
                status_message=str(request_exception),
                timings=timings,
            ), False, timings

    async def _connect_to_url_location(
        self,
        request_url: str,
        ssl_redirect_url: Optional[str]=None
    ) -> Tuple[
        HTTP3Connection,
        URL,
        bool
    ]:
        
        if ssl_redirect_url:
            parsed_url = URL(ssl_redirect_url)
        
        else:
            parsed_url = URL(request_url)

        url = self._url_cache.get(parsed_url.hostname)
        dns_lock = self._dns_lock[parsed_url.hostname]
        dns_waiter = self._dns_waiters[parsed_url.hostname]

        do_dns_lookup = url is None or ssl_redirect_url
        
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

        connection = self._connections.pop()

        if url.address is None or ssl_redirect_url:
            for address, ip_info in url:

                try:
                    
                    await connection.make_connection(
                        url.hostname,
                        address,
                        url.port,
                        ip_info,
                        ssl=self._client_ssl_context if url.is_ssl or ssl_redirect_url else None,
                        ssl_upgrade=ssl_redirect_url is not None
                    )

                    url.address = address
                    url.socket_config = ip_info

                except Exception as connection_error:
                    if 'server_hostname is only meaningful with ssl' in str(connection_error):
                        return None, parsed_url, True
                    
        else:
            try:
                    
                await connection.make_connection(
                    url.hostname,
                    url.address,
                    url.port,
                    url.socket_config,
                    ssl=self._client_ssl_context if url.is_ssl or ssl_redirect_url else None,
                    ssl_upgrade=ssl_redirect_url is not None
                )

            except Exception as connection_error:
                if 'server_hostname is only meaningful with ssl' in str(connection_error):
                    return None, parsed_url, True
                
                raise connection_error

        return connection, parsed_url, False