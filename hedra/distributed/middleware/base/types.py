from enum import Enum
from hedra.distributed.models.http import (
    Request,
    Response
)
from pydantic import BaseModel
from typing import (
    Any,
    Callable, 
    Union, 
    Tuple,
    Coroutine
)


class MiddlewareType(Enum):
    BIDIRECTIONAL='BIDIRECTIONAL'
    CALL='CALL'
    UNIDIRECTIONAL_BEFORE='UNIDIRECTIONAL_BEFORE'
    UNIDIRECTIONAL_AFTER='UNIDIRECTIONAL_AFTER'


RequestHandler = Callable[
    [Request],
    Coroutine[
        Any,
        Any,
        Tuple[
            Union[
                Response,
                BaseModel,
                str,
                None
            ],
            int
        ]
    ]
]

WrappedHandler = Callable[
    [
        Request,
        Response,
        int
    ],
    Coroutine[
        Any,
        Any,
        Tuple[
            Response,
            int
        ]
    ]
]

CallHandler = Callable[
    [
        Request,
        RequestHandler
    ],
    Coroutine[
        Any, 
        Any, 
        Tuple[
            Request,
            Response, 
            int
        ]
    ]
]

MiddlewareHandler = Callable[
    [
        Request,
        Response,
        int
    ],
    Coroutine[
        Any, 
        Any, 
        Tuple[
            Tuple[Response, int],
            bool
        ]
    ]
]



BidirectionalMiddlewareHandler = Callable[
    [
        Request,
        Response,
        int
    ],
    Coroutine[
        Any, 
        Any, 
        Tuple[
            Tuple[
                Request,
                Response, 
                int
            ],
            bool
        ]
    ]
]



Handler = Union[RequestHandler, WrappedHandler]