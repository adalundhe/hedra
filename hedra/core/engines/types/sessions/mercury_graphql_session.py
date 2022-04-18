from mercury_http.graphql import MercuryGraphQLClient
from mercury_http.common import Timeouts


class MercuryGraphQLSession:

    def __init__(self, pool_size=None, request_timeout=None, hard_cache=False, use_http2=False, reset_connections: bool=False) -> None:
        self._connection_pool_size = pool_size
        self.request_timeout = request_timeout
        self.hard_cache = hard_cache
        self.use_http2 = use_http2
        self._reset_connections = reset_connections

    async def create(self) -> MercuryGraphQLClient:

        if self.request_timeout is None:
            self.request_timeout = 60
        
        timeouts = Timeouts(total_timeout=self.request_timeout)

        return MercuryGraphQLClient(
            concurrency=self._connection_pool_size,
            timeouts=timeouts,
            hard_cache=self.hard_cache,
            use_http2=self.use_http2,
            reset_connections=self._reset_connections
        )
        

