import json
from typing import Dict, Any, Dict
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.engines.types.graphql_http2 import GraphQLHTTP2Result


class GraphQLHTTP2ProcessedResult(GraphQLHTTP2Result):

    def __init__(
        self, 
        stage: Any, 
        result: GraphQLHTTP2Result
    ) -> None:
        super(GraphQLHTTP2ProcessedResult, self).__init__(
            stage,
            result
        )
