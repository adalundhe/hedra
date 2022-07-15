import math
from random import randrange
from typing import List
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.common.types import RequestTypes
from .connection import HTTP2Connection
from .stream import AsyncStream


class HTTP2Pool:

    def __init__(self, size: int, timeouts: Timeouts, reset_connections: bool=False) -> None:
        self.size = size
        self.connections: List[HTTP2Connection] = []
        self.streams: List[AsyncStream] = []
        self.timeouts = timeouts
        self.pools_count = math.ceil(size/100)
        self.reset_connections = reset_connections
        self.pool_type: RequestTypes = RequestTypes.HTTP2

    def create_pool(self) -> None:

        self.connections = [
            HTTP2Connection(self.size) for _ in range(self.size)
        ]
        
        self.streams = [
            AsyncStream(
                randrange(1, 2**20 + 2, 2), 
                self.timeouts, 
                self.size, 
                self.reset_connections,
                stream_type=self.pool_type
            ) for _ in range(0, self.size * 2, 2)
        ]

    async def close(self):
        for connection in self.connections:
            await connection.close()
