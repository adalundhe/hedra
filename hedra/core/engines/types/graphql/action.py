import json
from typing import Dict, Iterator, Union, List
from hedra.core.engines.types.common.hooks import Hooks
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.http.action import HTTPAction
try:
    from graphql import Source, parse, print_ast

except ImportError:
    Source=None
    parse=lambda: None
    print_ast=lambda: None


class GraphQLAction(HTTPAction):

    def __init__(
        self,
        name: str, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        data: Union[str, dict, Iterator, bytes, None] = None, 
        user: str=None, 
        tags: List[Dict[str, str]] = [],
        redirects: int=10
    ) -> None:

        super(
            GraphQLAction,
            self
        ).__init__(
            name, 
            url, 
            method, 
            headers, 
            data, 
            user, 
            tags
        )

        self.type = RequestTypes.GRAPHQL
        self.redirects = redirects
        self.hooks: Hooks[GraphQLAction] = Hooks()

    def _setup_data(self) -> None:
        source = Source(self._data.get("query"))
        document_node = parse(source)
        query_string = print_ast(document_node)
        
        self.size = len(query_string)
        
        query = {
            "query": query_string
        }

        operation_name = self._data.get("operation_name")
        variables = self._data.get("variables")
        
        if operation_name:
            query["operationName"] = operation_name
        
        if variables:
            query["variables"] = variables

        self.encoded_data = json.dumps(query).encode()