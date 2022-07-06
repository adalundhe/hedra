from types import FunctionType
from typing import Any, Coroutine, Dict, List
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.common.types import RequestTypes
from .base import Action


class HTTPAction(Action):

    def __init__(
        self,  
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {}, 
        data: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType] = []
    ) -> None:
        super().__init__()
        self.url = url
        self.method = method
        self.headers = headers
        self.params = params
        self.data = data
        self.user = user
        self.tags = tags
        self.checks = checks

    def to_type(self, name: str):
        self.parsed = Request(
            name,
            self.url,
            method=self.method,
            headers=self.headers,
            params=self.params,
            payload=self.data,
            user=self.user,
            tags=self.tags,
            checks=self.checks,
            before=self.before,
            after=self.after,
            request_type=RequestTypes.HTTP
        )