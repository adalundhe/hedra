from types import FunctionType
from typing import Any, Coroutine, Dict, List
from hedra.core.engines.types.common import Request
from hedra.core.engines.types.common.types import RequestTypes
from .base import Action


class HTTPAction(Action):

    def __init__(
        self, 
        name: str, 
        url: str, 
        method: str = 'GET', 
        headers: Dict[str, str] = {}, 
        params: Dict[str, str] = {}, 
        data: Any = None, 
        user: str = None, 
        tags: List[Dict[str, str]] = [], 
        checks: List[FunctionType] = [],
        before: Coroutine = None,
        after: Coroutine = None,
    ) -> None:
        self.data = Request(
            name,
            url,
            method=method,
            headers=headers,
            params=params,
            payload=data,
            user=user,
            tags=tags,
            checks=checks,
            before=before,
            after=after,
            request_type=RequestTypes.HTTP
        )
