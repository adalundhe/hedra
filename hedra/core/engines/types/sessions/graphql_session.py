import asyncio
import uvloop
import logging
from aiohttp.connector import TCPConnector
from aiohttp.resolver import AsyncResolver
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.aiohttp import log as aiohttp_logger
from .http_session import HttpSession

aiohttp_logger.setLevel(logging.ERROR)


class GraphQLSession(HttpSession):

    def __init__(self, pool_size=None, dns_cache_seconds=None, session_url=None, request_timeout=None) -> None:
        super(GraphQLSession, self).__init__(
            pool_size=pool_size,
            dns_cache_seconds=dns_cache_seconds,
            request_timeout=request_timeout
        )

        self.session_url = session_url

    async def create(self) -> AIOHTTPTransport:
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        loop = asyncio.get_event_loop()

        connector = TCPConnector(
            limit=self.connection_pool_size, 
            ttl_dns_cache=self.dns_cache_seconds, 
            resolver=AsyncResolver(),
            keepalive_timeout=self.dns_cache_seconds,
            loop=loop
        )

        if self.request_timeout:
            transport = AIOHTTPTransport(
                url=self.session_url, 
                timeout=self.request_timeout, 
                client_session_args={
                    'connector': connector
                }
            )
        
        else:
            transport = AIOHTTPTransport(
                url=self.session_url, 
                client_session_args={
                    'connector': connector
                }
            )

        await transport.connect()
        return transport