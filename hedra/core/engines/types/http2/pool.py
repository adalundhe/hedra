from random import randrange
from typing import List
from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.common.types import RequestTypes
from .pipe import HTTP2Pipe
from .connection import HTTP2Connection


class HTTP2Pool:

    __slots__ = (
        'size',
        'connections',
        'pipes',
        'timeouts',
        'reset_connections',
        'pool_type'
    )

    def __init__(self, size: int, timeouts: Timeouts, reset_connections: bool=False) -> None:
        self.size = size
        self.connections: List[HTTP2Connection] = []
        self.pipes: List[HTTP2Pipe] = []
        self.timeouts = timeouts
        self.reset_connections = reset_connections
        self.pool_type: RequestTypes = RequestTypes.HTTP2

    def create_pool(self) -> None:

        self.pipes = [ HTTP2Pipe(self.size) for _ in range(self.size) ]
        
        self.connections = [
            HTTP2Connection(
                randrange(1, 2**20 + 2, 2), 
                self.timeouts, 
                self.size, 
                self.reset_connections,
                self.pool_type
            ) for _ in range(0, self.size * 2, 2)
        ]

    def reset(self):
        self.pipes.append(HTTP2Pipe(self.size))

        self.connections.append(
            HTTP2Connection(
                randrange(1, 2**20 + 2, 2), 
                self.timeouts, 
                self.size, 
                self.reset_connections,
                self.pool_type
            )
        )

    async def close(self):
        for connection in self.connections:
            await connection.close()
