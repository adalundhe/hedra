import math
from typing import List
from hedra.core.engines.types.common.timeouts import Timeouts
from .connection import HTTP2Connection
from .stream import AsyncStream


class HTTP2Pool:

    def __init__(self, size: int, timeouts: Timeouts, reset_connections=False) -> None:
        self.size = size
        self.connections: List[HTTP2Connection] = []
        self.timeouts = timeouts
        self.pools_count = math.ceil(size/100)
        self.reset_connections = reset_connections

    def create_pool(self) -> None:
        self.connections = [
            AsyncStream(idx, self.timeouts, self.size, self.reset_connections) for idx in range(0, self.size * 2, 2)
        ]

    async def close(self):
        for connection in self.connections:
            await connection.close()
