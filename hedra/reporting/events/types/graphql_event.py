from hedra.core.engines.types.graphql import GraphQLResult
from .http_event import HTTPEvent


class GraphQLEvent(HTTPEvent):

    def __init__(self, result: GraphQLResult) -> None:
        super(GraphQLEvent, self).__init__(result)