from typing import Any, Tuple, Dict
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.engines.types.graphql import GraphQLResult
from .http_event import HTTPEvent


class GraphQLEvent(HTTPEvent):

    def __init__(
        self, 
        stage: Any, 
        result: GraphQLResult
    ) -> None:
        super(GraphQLEvent, self).__init__(
            stage,
            result
        )