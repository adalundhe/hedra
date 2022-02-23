from __future__ import annotations
from .models import Connection


class StatStreamConnector:

    def __init__(self, config):
        self.config = config
        self.connection = Connection(self.config)

    def connect(self) -> StatStreamConnector:
        self.connection.connect()
        return self

    def setup(self) -> StatStreamConnector:
        return self

    def execute(self, query) -> StatStreamConnector:
        self.connection.execute(query)
        return self

    def commit(self) -> list:
        return self.connection.commit()

    def clear(self) -> StatStreamConnector:
        self.connection.clear()
        return self

    def close(self) -> StatStreamConnector:
        self.connection.close()
        return self
