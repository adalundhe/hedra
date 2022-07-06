from types import FunctionType
from typing import Any, Dict, List
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.common.types import RequestTypes
from .base import Action


class GraphQLAction(Action):

    def __init__(
        self, 
        url: str, 
        query: str,
        operation_name: str = None,
        variables: Dict[str, Any] = None, 
        headers: Dict[str, str] = {}, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType]=[]
    ) -> None:
        super().__init__()
        self.url = url
        self.query = query
        self.operation_name = operation_name
        self.variables = variables
        self.headers = headers
        self.user = user
        self.tags = tags
        self.checks = checks

    def to_type(self, name: str):
        self.payload = Request(
            name,
            self.url,
            method='POST',
            headers=self.headers,
            payload={
                "query": self.query,
                "operation_name": self.operation_name,
                "variables": self.variables
            },
            user=self.user,
            tags=self.tags,
            checks=self.checks,
            request_type=RequestTypes.GRAPHQL
        )

    @classmethod
    def about(cls):
        return '''
        Mercury-GraphQL Action

        Mercury-GraphQL Actions represent a single HTTP/1 or HTTP/2 REST call using Hedra's 
        Mercury-GraphQL engine. For example - a GET request to https://www.google.com/.

        Actions are specified as:

        - endpoint: <host_endpoint>
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - url: (optional) <full_target_url> (overrides host and endpoint if provided)
        - method: <rest_request_method>
        - headers: <rest_request_headers>
        - params: <rest_request_params>
        - data: <rest_request_data>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>

        '''