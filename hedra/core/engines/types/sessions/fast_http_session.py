from.http_session import HttpSession
from aiosonic import HTTPClient
from aiosonic.connectors import TCPConnector
from aiosonic.timeout import Timeouts
from aiosonic.resolver import AsyncResolver


class FastHttpSession(HttpSession):

    def __init__(self, pool_size=None, dns_cache_seconds=None, request_timeout=None) -> None:
        super().__init__(
            pool_size=pool_size,
            dns_cache_seconds=dns_cache_seconds,
            request_timeout=request_timeout
        )

    async def create(self) -> HTTPClient:
        timeouts = Timeouts(request_timeout=self.request_timeout)

        if self.request_timeout:
            connector = TCPConnector(
                pool_size=self.connection_pool_size, 
                ttl_dns_cache=self.dns_cache_seconds, 
                timeouts=timeouts,
                resolver=AsyncResolver()
            )

        else:
            connector = TCPConnector(
                pool_size=self.connection_pool_size, 
                ttl_dns_cache=self.dns_cache_seconds,
                resolver=AsyncResolver()
            )

        return HTTPClient(connector) 