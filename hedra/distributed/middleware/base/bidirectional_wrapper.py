from hedra.distributed.models.http import Request
from pydantic import BaseModel
from typing import (
    Callable, 
    Union, 
    Dict, 
    Optional,
    List,
    Literal,
    TypeVar
)
from .base_wrapper import BaseWrapper
from .types import MiddlewareType
from .types import (
    Handler,
    BidirectionalMiddlewareHandler
)


T = TypeVar('T')


class BidirectionalWrapper(BaseWrapper):

    def __init__(
        self,
        name: str,
        handler: Handler,
        middleware_type: MiddlewareType=MiddlewareType.BIDIRECTIONAL,
        methods: Optional[
            List[
                Literal[
                    "GET",
                    "HEAD",
                    "OPTIONS",
                    "POST",
                    "PUT",
                    "PATCH",
                    "DELETE",
                    "TRACE"
                ]
            ]
        ]=None,
        responses: Optional[
            Dict[
                int,
                BaseModel
            ]
        ]=None,
        serializers: Optional[
            Dict[
                int,
                Callable[
                    ...,
                    str
                ]
            ]
        ]=None,
        response_headers: Optional[
            Dict[str, str]
        ]=None
    ) -> None:
        
        super().__init__()

        self.name = name
        self.path  = handler.path
        self.methods: List[
            Literal[
                "GET",
                "HEAD",
                "OPTIONS",
                "POST",
                "PUT",
                "PATCH",
                "DELETE",
                "TRACE"
            ]
        ] = handler.methods

        if methods:
            self.methods.extend(methods)

        self.response_headers: Union[
            Dict[str, str],
            None
        ] = handler.response_headers

        if self.response_headers and response_headers:
            self.response_headers.update(response_headers)

        elif response_headers:
            self.response_headers = response_headers

        self.responses = responses
        self.serializers = serializers
        self.limit = handler.limit

        self.handler = handler
        self.wraps = isinstance(handler, BaseWrapper)

        if self.handler.response_headers and self.response_headers:
            self.handler.response_headers = {}

        self.pre: Optional[BidirectionalMiddlewareHandler] = None
        self.post: Optional[BidirectionalMiddlewareHandler] = None

        self.middleware_type = middleware_type

    async def __call__(
        self, 
        request: Request
    ):

        (request, response, middleware_status), run_next = await self.pre(
            request,
            None,
            None
        )

        if run_next is False:
            return response, middleware_status
        
        if self.wraps:
            result, status = await self.handler(request)
            result.headers.update(response.headers)

        else:
            result, status = await self.handler(request)
            

        (request, response, middleware_status), run_next = await self.post(
            request,
            result,
            status
        )

        if run_next is False:
            return response, middleware_status
        
        return response, status