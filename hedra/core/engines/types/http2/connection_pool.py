from .connection import HTTP2Connection


class ConnectionPool:

    def __init__(self, size) -> None:
        self.size = size
        self.connections = []

    def create_pool(self):
        self.connections = [HTTP2Connection(self.size) for _ in range(self.size)]
    