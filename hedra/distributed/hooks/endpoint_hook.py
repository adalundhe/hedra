import functools
from hedra.distributed.models.http import (
    Request,
    Limit
)
from pydantic import BaseModel
from typing import (
    Optional, 
    List, 
    Literal,
    Dict,
    Callable,
    Tuple,
    TypeVar,
    Any
)

T = TypeVar('T')


def endpoint(
    path: Optional[str]="/",
    methods: List[
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
    ]=["GET"],
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
    middleware: Optional[
        List[
            Callable[
                [
                    Request
                ],
                Tuple[
                    Any,
                    int,
                    bool
                ]
            ]
        ]
    ]=None,
    response_headers: Optional[Dict[str, str]]=None,
    limit: Optional[Limit]=None
):

    def wraps(func):

        func.server_only = True
        func.path = path
        func.methods = methods
        func.as_http = True
        
        func.response_headers = response_headers or {}
        func.responses = responses
        func.serializers = serializers
        func.limit = limit
        
        if middleware:
            @functools.wraps(func)
            async def middleware_decorator(
                *args,
                **kwargs
            ):
            
                run_next = True

                _, request = args

                for middleware_func in middleware:
                    response, run_next = await middleware_func(request)

                    if run_next is False:
                        return response
                    

                return await func(*args, **kwargs)
            
            return middleware_decorator
        

        else:
            @functools.wraps(func)
            def decorator(
                *args,
                **kwargs
            ):
                return func(*args, **kwargs)
            
            return decorator
    
    return wraps
