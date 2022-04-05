import asyncio
import uvloop
import psutil
from aiohttp import (
    ClientSession,
    ClientTimeout
)
from aiohttp.connector import TCPConnector
from aiohttp.resolver import AsyncResolver

class HttpSession:

    def __init__(self, pool_size=None, dns_cache_seconds=None, request_timeout=None) -> None:
        self.connection_pool_size = int((psutil.cpu_count(logical=True) * 10**3)/pool_size)
        self.dns_cache_seconds = dns_cache_seconds
        self.request_timeout = request_timeout

    async def create(self) -> ClientSession:
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
            timeout = ClientTimeout(self.request_timeout)
            return ClientSession(connector=connector, timeout=timeout)
        
        return ClientSession(connector=connector)
