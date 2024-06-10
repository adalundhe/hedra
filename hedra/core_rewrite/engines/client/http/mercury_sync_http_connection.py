from __future__ import annotations

import asyncio
import ssl
import time
from collections import defaultdict
from typing import Dict, List, Literal, Optional, Tuple, Union
from urllib.parse import urlparse

from pydantic import BaseModel

from hedra.core_rewrite.engines.client.shared.models import (
    URL,
    Cookies,
    HTTPCookie,
    HTTPEncodableValue,
    Metadata,
    URLMetadata,
)

from .models.http import (
    HTTPRequest,
    HTTPResponse,
)
from .protocols import HTTPConnection
from .timeouts import Timeouts


class MercurySyncHTTPConnection:

    def __init__(
        self, 
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        pool_size: Optional[int]=None,
    ) -> None:
        
        if pool_size is None:
            pool_size = 100
        
        self.timeouts = Timeouts()

        self._cert_path = cert_path
        self._key_path = key_path
        self._client_ssl_context = self._create_general_client_ssl_context()
        
        self._dns_lock: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: Dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: List[asyncio.Future] = []

        self._client_waiters: Dict[asyncio.Transport, asyncio.Future] = {}
        self._connections: List[HTTPConnection] = [
            HTTPConnection() for _ in range(pool_size)
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
                        HTTPRequest(
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

                return HTTPResponse(
                    metadata=Metadata(),
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
                        HTTPRequest(
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

                return HTTPResponse(
                    metadata=Metadata(),
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
                        HTTPRequest(
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

                return HTTPResponse(
                    metadata=Metadata(),
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
                        HTTPRequest(
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

                return HTTPResponse(
                    metadata=Metadata(),
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
                        HTTPRequest(
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

                return HTTPResponse(
                    metadata=Metadata(),
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
                        HTTPRequest(
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

                return HTTPResponse(
                    metadata=Metadata(),
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
                        HTTPRequest(
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

                return HTTPResponse(
                    metadata=Metadata(),
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
        request: HTTPRequest, 
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

            location = result.headers.get(
                b'location'
            ).decode()

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

                location = result.headers.get(b'location').decode()

                upgrade_ssl = False
                if 'https' in location and 'https' not in request.url:
                    upgrade_ssl = True

        timings['request_end'] = time.monotonic()
        result.timings.update(timings)

        return result
        
    async def _execute(
        self,
        request: HTTPRequest,
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
        HTTPResponse, 
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

            headers, data = request.prepare(url)

            if connection.reader is None:
                
                timings['connect_end'] = time.monotonic()

                return HTTPResponse(
                    url=URLMetadata(
                        host=url.hostname,
                        path=url.path
                    ),
                    method=request.method,
                    status=400,
                    headers=headers
                ), False, timings
            

            timings['connect_end'] = time.monotonic()

            if timings['write_start'] is None:
                timings['write_start'] = time.monotonic()

            connection.write(headers)

            if data:
                connection.write(data)

            timings['write_end'] = time.monotonic()

            if timings['read_start'] is None:
                timings['read_start'] = time.monotonic()

            response_code = await asyncio.wait_for(
                connection.reader.readline(),
                timeout=self.timeouts.read_timeout
            )

            headers: Dict[bytes, bytes] = await asyncio.wait_for(
                connection.read_headers(),
                timeout=self.timeouts.read_timeout
            )

            status_string: List[bytes] = response_code.split()
            status = int(status_string[1])

            if status >= 300 and status < 400:
                timings['read_end'] = time.monotonic()

                return HTTPResponse(
                    url=URLMetadata(
                        host=url.hostname,
                        path=url.path
                    ),
                    method=request.method,
                    status=status,
                    headers=headers
                ), True, timings

            content_length = headers.get(b'content-length')
            transfer_encoding = headers.get(b'transfer-encoding')

            cookies: Union[Cookies, None] = None
            cookies_data: Union[bytes, None] = headers.get(b'set-cookie')
            if cookies_data:
                cookies = Cookies()
                cookies.update(cookies_data)

            # We require Content-Length or Transfer-Encoding headers to read a
            # request body, otherwise it's anyone's guess as to how big the body
            # is, and we ain't playing that game.
            
            if content_length:
                body = await asyncio.wait_for(
                    connection.readexactly(int(content_length)),
                    timeout=self.timeouts.read_timeout
                )

            elif transfer_encoding:
                
                body = bytearray()
                all_chunks_read = False

                while True and not all_chunks_read:
                    chunk_size = int(
                        (
                            await asyncio.wait_for(
                                connection.readline(),
                                timeout=self.timeouts.read_timeout
                            )
                        ).rstrip(), 
                        16
                    )
                    
                    if not chunk_size:
                        # read last CRLF
                        await asyncio.wait_for(
                            connection.readline(),
                            timeout=self.timeouts.read_timeout
                        )
                        break

                    chunk = await asyncio.wait_for(
                        connection.readexactly(chunk_size + 2),
                        self.timeouts.read_timeout
                    )
                    body.extend(
                        chunk[:-2]
                    )

                all_chunks_read = True

            self._connections.append(connection)

            timings['read_end'] = time.monotonic()

            return HTTPResponse(
                url=URLMetadata(
                    host=url.hostname,
                    path=url.path
                ),
                cookies=cookies,
                method=request.method,
                status=status,
                headers=headers,
                content=body
            ), False, timings

        except Exception as request_exception:
            self._connections.append(
                HTTPConnection()
            )

            if isinstance(request_url, str):
                request_url = urlparse(request_url)

            timings['read_end'] = time.monotonic()

            return HTTPResponse(
                url=URLMetadata(
                    host=request_url.hostname,
                    path=request_url.path
                ),
                method=request.method,
                status=400,
                status_message=str(request_exception)
            ), False, timings

    async def _connect_to_url_location(
        self,
        request_url: str,
        ssl_redirect_url: Optional[str]=None
    ) -> Tuple[
        HTTPConnection,
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
