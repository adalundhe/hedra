from typing import List, Type
from .connection import CustomConnection


class CustomPool:

    __slots__ = (
        'size',
        'connections',
        'reset_connections',
        'create_connection_type'
    )

    def __init__(self, create_connection_type: Type[CustomConnection], size: int, reset_connections: bool = False) -> None:
        self.size = size
        self.connections: List[CustomConnection] = []
        self.reset_connections = reset_connections
        self.create_connection_type: CustomConnection = create_connection_type

    def create_pool(self) -> None:
        for _ in range(self.size):
            self.connections.append(
                self.create_connection_type(self.reset_connections)
            )

    async def close(self):
        for connection in self.connections:
            await connection.close()