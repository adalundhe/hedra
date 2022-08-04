import json
from types import FunctionType
from typing import Coroutine, Dict, Iterator, Union, List
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.http.action import HTTPAction
from graphql import Source, parse, print_ast


class GraphQLAction(HTTPAction):

    def __init__(
        self,
        name: str, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        data: Union[str, dict, Iterator, bytes, None] = None, 
        user: str=None, 
        tags: List[Dict[str, str]] = []
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