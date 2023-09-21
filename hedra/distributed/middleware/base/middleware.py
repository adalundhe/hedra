from __future__ import annotations
from hedra.distributed.models.http import (
    Request,
    Response
)
from pydantic import BaseModel
from typing import (
    Callable, 
    Union, 
    Dict, 
    Optional,
    List,
    Literal,
    Tuple
)
from .bidirectional_wrapper import BidirectionalWrapper
from .call_wrapper import CallWrapper
from .unidirectional_wrapper import UnidirectionalWrapper
from .types import MiddlewareType


class Middleware:

    def __init__(
        self,
        name: str,
        middleware_type: MiddlewareType=MiddlewareType.UNIDIRECTIONAL_BEFORE,
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
        response_headers: Dict[str, str]={}
    ) -> None:
        
        self.name = name
        self.methods = methods
        self.response_headers = response_headers
        self.middleware_type = middleware_type
        self.wraps = False

        self._wrapper_types = {
            MiddlewareType.BIDIRECTIONAL: BidirectionalWrapper,
            MiddlewareType.CALL: CallWrapper,
            MiddlewareType.UNIDIRECTIONAL_BEFORE: UnidirectionalWrapper,
            MiddlewareType.UNIDIRECTIONAL_AFTER: UnidirectionalWrapper,
        }

    def __call__(self, request: Request) -> Tuple[
        Tuple[Response, int],
        bool
    ]:
        raise NotImplementedError('Err. __call__() should not be called on base Middleware class.')

    def wrap(
        self,
        handler: Callable[
            [Request],
            Union[
                BaseModel,
                str,
                None
            ]
        ]
    ):
        
        wrapper = self._wrapper_types.get(
            self.middleware_type,
            BidirectionalWrapper(
                self.name,
                handler,
                methods=self.methods,
                response_headers=self.response_headers,
                middleware_type=self.middleware_type 
            )
        )(
            self.name,
            handler,
            methods=self.methods,
            response_headers=self.response_headers,
            middleware_type=self.middleware_type 
        )

        if isinstance(wrapper, BidirectionalWrapper):
            wrapper.pre = self.__pre__
            wrapper.post = self.__post__

        elif isinstance(wrapper, (CallWrapper, UnidirectionalWrapper)):

            wrapper.run = self.__run__
        
            self.response_headers.update(wrapper.response_headers)

        wrapper.setup = self.__setup__
        self.wraps = wrapper.wraps
        
        return wrapper
    
    async def __setup__(self):
        pass
    
    async def __pre__(
        self, 
        request: Request,
        response: Response,
        status: int
    ) -> Tuple[
        Tuple[
            Request,
            Response, 
            int
        ],
        bool
    ]:
        raise NotImplementedError('Err. - __pre__() is not implemented for base Middleware class.')
    
    async def __post__(
        self, 
        request: Request,
        response: Response,
        status: int
    ) -> Tuple[
        Tuple[
            Request,
            Response, 
            int
        ],
        bool
    ]:
        raise NotImplementedError('Err. - __post__() is not implemented for base Middleware class.')
    
    async def __run__(
        self, 
        request: Request,
        response: Response,
        status: int
    ) -> Tuple[
        Tuple[Response, int],
        bool
    ]:
        raise NotImplementedError('Err. - __post__() is not implemented for base Middleware class.')
    
    async def run(
        self,
        request: Request
    ):
        raise NotImplementedError('Err. - middleware() is not implemented for base Middleware class.')
    