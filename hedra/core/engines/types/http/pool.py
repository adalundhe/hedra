
from typing import List
from .connection import Connection


class Pool:

    def __init__(self, size: int, reset_connections: bool = False) -> None:
        self.size = size
        self.connections: List[Connection] = []
        self.reset_connections = reset_connections

    def create_pool(self) -> None:
        for _ in range(self.size):
            self.connections.append(
                Connection(self.reset_connections)
            )
