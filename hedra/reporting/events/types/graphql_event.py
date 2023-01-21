from typing import Any
from hedra.core.engines.types.graphql import GraphQLResult
from .http_event import HTTPEvent


class GraphQLEvent(HTTPEvent):

    def __init__(self, stage: Any, result: GraphQLResult) -> None:
        super(GraphQLEvent, self).__init__(
            stage,
            result
        )