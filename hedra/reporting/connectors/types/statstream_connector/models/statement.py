from .types import (
    Query,
    Event,
    StreamConfig
)


class Statement:

    def __init__(self, statement):
        self.statement = statement
        self.type = statement.get('type', 'query')

    def to_query(self) -> Query:
        return Query(self.statement)

    def to_event(self) -> Event:
        return Event(self.statement)

    def to_stream_config(self) -> StreamConfig:
        return StreamConfig(self.statement)