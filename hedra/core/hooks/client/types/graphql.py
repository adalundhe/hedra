from types import FunctionType
from typing import Any, Dict, List
from hedra.core.hooks.client.config import Config
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.common.hooks import Hooks
from hedra.core.engines.types.graphql.client import MercuryGraphQLClient
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.common import Timeouts
from .base_client import BaseClient


class GraphQLClient(BaseClient):

    def __init__(self, config: Config) -> None:
        super().__init__()
        
        self.session = MercuryGraphQLClient(
            concurrency=config.batch_size,
            timeouts=Timeouts(
                total_timeout=config.request_timeout
            ),
            reset_connections=config.options.get('reset_connections')
        )
        self.request_type = RequestTypes.GRAPHQL
        self.next_name = None

    def __getitem__(self, key: str):
        return self.session.registered.get(key)

    async def query(
        self,
        url: str, 
        query: str,
        operation_name: str = None,
        variables: Dict[str, Any] = None, 
        headers: Dict[str, str] = {}, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType]=[]
    ):
        if self.session.protocol.registered.get(self.next_name) is None:
            result = await self.session.prepare(
                Request(
                    self.next_name,
                    url,
                    method='POST',
                    headers=headers,
                    payload={
                        "query": query,
                        "operation_name": operation_name,
                        "variables": variables
                    },
                    user=user,
                    tags=tags,
                    checks=checks,
                    request_type=self.request_type
                )
            )

            if isinstance(result, Exception):
                raise result

        return self.session
        
