import httpx
from .http_session import HttpSession


class Http2Session(HttpSession):

    def __init__(self, pool_size=None, request_timeout=None) -> None:
        super(Http2Session, self).__init__(
            pool_size=pool_size,
            request_timeout=request_timeout
        )

    async def create(self) -> httpx.AsyncClient:

        limits = httpx.Limits(
            max_keepalive_connections=self.connection_pool_size,
            max_connections=self.connection_pool_size
        )

        if self.request_timeout:
            timeout = httpx.Timeout(self.request_timeout)
            return httpx.AsyncClient(http2=True, timeout=timeout, limits=limits)
        
        return httpx.AsyncClient(http2=True, limits=limits)