from mercury_http.grpc import MercuryGRPCClient
from mercury_http.common import Timeouts


class MercuryGRPCSession:

    def __init__(self, pool_size=None, request_timeout=None, hard_cache=False, reset_connections: bool=False) -> None:
        self._connection_pool_size = pool_size
        self.request_timeout = request_timeout
        self.hard_cache = hard_cache
        self._reset_connections = reset_connections

    async def create(self) -> MercuryGRPCClient:

        if self.request_timeout is None:
            self.request_timeout = 60
        
        timeouts = Timeouts(total_timeout=self.request_timeout)

        return MercuryGRPCClient(
            concurrency=self._connection_pool_size,
            timeouts=timeouts,
            hard_cache=self.hard_cache,
            reset_connections=self._reset_connections
        )
        