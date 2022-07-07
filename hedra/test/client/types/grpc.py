import inspect
from types import FunctionType
from typing import Any, Dict, List
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.grpc.client import MercuryGRPCClient


class GRPCClient:
    
    def __init__(self, session: MercuryGRPCClient) -> None:
        self.session = session
        self.request_type = RequestTypes.GRPC

    def __getitem__(self, key: str):
        return self.session.registered.get(key)

    async def request(
        self, 
        url: str, 
        headers: Dict[str, str] = {}, 
        protobuf: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType]=[]
    ):
        if self.session.registered.get(self.next_name) is None:
            result = await self.session.prepare(
                Request(
                    self.next_name,
                    url,
                    method='POST',
                    headers=headers,
                    payload=protobuf,
                    user=user,
                    tags=tags,
                    checks=checks,
                    request_type=self.request_type
                )
            )

            if result and result.error:
                raise result.error
