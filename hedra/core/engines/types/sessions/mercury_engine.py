from mercury_http.http import MercuryHTTPClient
from mercury_http.http.timeouts import Timeouts
from .http_session import HttpSession

class MercuryHTTPSession(HttpSession):

    def __init__(self, pool_size=None, request_timeout=None, hard_cache=False) -> None:
        super().__init__(
            pool_size=pool_size,
            request_timeout=request_timeout
        )
        self.connection_pool_size = pool_size
        self.hard_cache = hard_cache

    async def create(self) -> MercuryHTTPClient:

        if self.request_timeout is None:
            self.request_timeout = 60
        
        timeouts = Timeouts(total_timeout=self.request_timeout)

        return MercuryHTTPClient(
            concurrency=self.connection_pool_size,
            timeouts=timeouts,
            hard_cache=self.hard_cache
        )
        

