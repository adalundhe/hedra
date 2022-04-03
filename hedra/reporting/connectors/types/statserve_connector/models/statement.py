from .types import (
    Event,
    FieldStatsSummary,
    Query,
    StreamSummary,
    UpdateResponse
)


class Statement:

    def __init__(self, statement):
        self.statement = statement
        self.type = statement.get('type', 'query')

    async def to_query(self) -> Query:
        return Query(self.statement)

    async def to_event(self) -> Event:
        return Event(self.statement)

    async def to_field_stats_summary(self) -> FieldStatsSummary:
        return FieldStatsSummary(self.statement)

    async def to_stream_summary(self) -> StreamSummary:
        return StreamSummary(self.statement)

    async def to_response(self) -> UpdateResponse:
        return UpdateResponse(self.statement)
