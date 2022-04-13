from mercury_http.http import MercuryHTTPClient
from mercury_http.http.timeouts import Timeouts


class MercuryHTTPSession:

    def __init__(self, pool_size=None, request_timeout=None, hard_cache=False) -> None:
        self._connection_pool_size = pool_size
        self.request_timeout = request_timeout
        self.hard_cache = hard_cache

    async def create(self) -> MercuryHTTPClient:

        if self.request_timeout is None:
            self.request_timeout = 60
        
        timeouts = Timeouts(total_timeout=self.request_timeout)

        return MercuryHTTPClient(
            concurrency=self._connection_pool_size,
            timeouts=timeouts,
            hard_cache=self.hard_cache
        )
        

