from mercury_http.websocket import MercuryWebsocketClient
from mercury_http.common.timeouts import Timeouts


class MercuryWebsocketSession:

    def __init__(self, pool_size=None, request_timeout=None, hard_cache=False, reset_connections: bool=False) -> None:
        self._connection_pool_size = pool_size
        self.request_timeout = request_timeout
        self.hard_cache = hard_cache
        self._reset_connections = reset_connections

    async def create(self) -> MercuryWebsocketClient:

        if self.request_timeout is None:
            self.request_timeout = 60
        
        timeouts = Timeouts(total_timeout=self.request_timeout)

        return MercuryWebsocketClient(
            concurrency=self._connection_pool_size,
            timeouts=timeouts,
            hard_cache=self.hard_cache,
            reset_connection=self._reset_connections
        )
        

