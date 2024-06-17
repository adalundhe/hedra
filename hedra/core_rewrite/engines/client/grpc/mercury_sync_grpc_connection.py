import asyncio
import ssl
import time
import uuid
from typing import (
    Dict,
    Literal,
    Optional,
    Tuple,
    TypeVar,
    Union,
)
from urllib.parse import urlparse

from hedra.core_rewrite.engines.client.http2 import MercurySyncHTTP2Connection
from hedra.core_rewrite.engines.client.http2.pipe import HTTP2Pipe
from hedra.core_rewrite.engines.client.http2.protocols import HTTP2Connection
from hedra.core_rewrite.engines.client.shared.models import (
    URL,
    URLMetadata,
)
from hedra.core_rewrite.engines.client.shared.timeouts import Timeouts

from .models.grpc import (
    GRPCRequest,
    GRPCResponse,
)

T = TypeVar('T')

class MercurySyncGRPCConnection(MercurySyncHTTP2Connection):

    def __init__(
        self, 
        pool_size: int = 10 ** 3, 
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool=False,
    ) -> None:
        super(
            MercurySyncGRPCConnection,
            self
        ).__init__(
            pool_size=pool_size,
            cert_path=cert_path,
            key_path=key_path,
            timeouts=timeouts, 
            reset_connections=reset_connections
        )

        self.session_id = str(uuid.uuid4())
        
    async def send(
        self,
        url: str,
        protobuf: T,
        timeout: Union[
            Optional[int], 
            Optional[float]
        ]=None,
        redirects: int=3
    ) -> GRPCResponse:
        async with self._semaphore:

            try:
                
                return await asyncio.wait_for(
                    self._request(
                        GRPCRequest(
                            url=url,
                            protobuf=protobuf,
                            redirects=redirects
                        ),
                    ),
                    timeout=timeout
                )
            
            except asyncio.TimeoutError:

                url_data = urlparse(url)

                return GRPCResponse(
                    url=URLMetadata(
                        host=url_data.hostname,
                        path=url_data.path,
                        params=url_data.params,
                        query=url_data.query
                    ),
                    status=408,
                    status_message='Request timed out.'
                )


    
    async def _request(
        self, 
        request: GRPCRequest, 
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
        request: GRPCRequest,
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
    ):
        
        if redirect_url:
            request_url = redirect_url

        else:
            request_url = request.url

        try:

            if timings['connect_start'] is None:
                timings['connect_start'] = time.monotonic()
            
            (
                error, 
                connection,
                pipe,
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

                (
                    error, 
                    connection,
                    pipe, 
                    url, 
                    _
                ) = await asyncio.wait_for(
                    self._connect_to_url_location(
                        request_url,
                        ssl_redirect_url=ssl_redirect_url
                    ),
                    timeout=self.timeouts.connect_timeout
                )

                request_url = ssl_redirect_url

            headers = request.encode_headers(url)

            if error:
                timings['connect_end'] = time.monotonic()

                self._connections.append(
                    HTTP2Connection(
                        self._concurrency,
                        stream_id=connection.stream.stream_id,
                        reset_connection=connection.reset_connection
                    )
                )

                self._pipes.append(
                    HTTP2Pipe(self._max_concurrency)
                )

                return GRPCResponse(
                    url=URLMetadata(
                        host=url.hostname,
                        path=url.path
                    ),
                    method='POST',
                    status=400,
                    headers={
                        key.encode(): value.encode() for key, value in request.headers.items()
                    }
                ), False, timings
            
            timings['connect_end'] = time.monotonic()

            if timings['write_start'] is None:
                timings['write_start'] = time.monotonic()

            connection = pipe.send_preamble(connection)
            data = request.encode_data()

            connection = pipe.send_request_headers(
                headers,
                data,
                connection
            )

            if data:
                connection = await asyncio.wait_for(
                    pipe.submit_request_body(
                        data,
                        connection
                    ),
                    timeout=self.timeouts.write_timeout
                )
            
            timings['write_end'] = time.monotonic()

            if timings['read_start'] is None:
                timings['read_start'] = time.monotonic()

            (
                status,
                headers,
                body,
                error
            ) = await asyncio.wait_for(
                pipe.receive_response(connection), 
                    timeout=self.timeouts.read_timeout
                )
            
            if status >= 300 and status < 400:
                timings['read_end'] = time.monotonic()

                self._connections.append(
                    HTTP2Connection(
                        self._concurrency,
                        connection.stream.stream_id,
                        reset_connection=connection.reset_connection
                    )
                )

                self._pipes.append(
                    HTTP2Pipe(self._max_concurrency)
                )

                return GRPCResponse(
                    url=URLMetadata(
                        host=url.hostname,
                        path=url.path
                    ),
                    method='POST',
                    status=status,
                    headers=headers
                ), True, timings
            
            if error:
                raise error

            self._connections.append(connection)
            self._pipes.append(pipe)

            timings['read_end'] = time.monotonic()

            return GRPCResponse(
                url=URLMetadata(
                    host=url.hostname,
                    path=url.path
                ),
                method='POST',
                status=status,
                headers=headers,
                content=body
            ), False, timings

        except Exception as request_exception:
            self._connections.append(
                HTTP2Connection(
                    self._concurrency,
                    connection.stream.stream_id,
                    reset_connection=connection.reset_connection
                )
            )

            self._pipes.append(
                HTTP2Pipe(self._max_concurrency)
            )

            if isinstance(request_url, str):
                request_url = urlparse(request_url)

            timings['read_end'] = time.monotonic()

            return GRPCResponse(
                url=URLMetadata(
                    host=request_url.hostname,
                    path=request_url.path
                ),
                method='POST',
                status=400,
                status_message=str(request_exception)
            ), False, timings
        
    def _create_http2_ssl_context(self):
        """
        This function creates an SSLContext object that is suitably configured for
        HTTP/2. If you're working with Python TLS directly, you'll want to do the
        exact same setup as this function does.
        """
        # Get the basic context from the standard library.
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)

        # RFC 7540 Section 9.2: Implementations of HTTP/2 MUST use TLS version 1.2
        # or higher. Disable TLS 1.1 and lower.
        ctx.options |= (
            ssl.OP_NO_SSLv2 | ssl.OP_NO_SSLv3 | ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
        )

        # RFC 7540 Section 9.2.1: A deployment of HTTP/2 over TLS 1.2 MUST disable
        # compression.
        ctx.options |= ssl.OP_NO_COMPRESSION

        # RFC 7540 Section 9.2.2: "deployments of HTTP/2 that use TLS 1.2 MUST
        # support TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256". In practice, the
        # blocklist defined in this section allows only the AES GCM and ChaCha20
        # cipher suites with ephemeral key negotiation.
        ctx.set_ciphers("ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20")

        # We want to negotiate using NPN and ALPN. ALPN is mandatory, but NPN may
        # be absent, so allow that. This setup allows for negotiation of HTTP/1.1.
        ctx.set_alpn_protocols(["h2", "http/1.1"])

        try:
            if hasattr(ctx, '_set_npn_protocols'):
                ctx.set_npn_protocols(["h2", "http/1.1"])
        except NotImplementedError:
            pass


        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        return ctx

    async def _connect_to_url_location(
        self,
        request_url: str,
        ssl_redirect_url: Optional[str]=None
    ) -> Tuple[
        Exception,
        HTTP2Connection,
        HTTP2Pipe,
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
        pipe = self._pipes.pop()

        connection_error: Optional[Exception] = None

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
                        return (
                            None, 
                            None,
                            None,
                            parsed_url, 
                            True
                        )
                    
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
                    return (
                        None,
                        None,
                        None,
                        parsed_url, 
                        True
                    )
                
                raise connection_error

        return (
            connection_error,
            connection,
            pipe, 
            parsed_url, 
            False
        )