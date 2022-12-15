
from typing import List
from .connection import UDPConnection


class Pool:

    __slots__ = (
        'size',
        'connections',
        'reset_connections'
    )

    def __init__(self, size: int, reset_connections: bool = False) -> None:
        self.size = size
        
        self.connections: List[UDPConnection] = []
        self.reset_connections = reset_connections

    def create_pool(self) -> None:
        for _ in range(self.size):
            self.connections.append(
                UDPConnection(self.reset_connections)
            )

    async def close(self):
        for connection in self.connections:
            await connection.close()
