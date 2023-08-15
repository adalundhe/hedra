from hedra.distributed.env.memory_parser import MemoryParser
from hedra.distributed.env.time_parser import TimeParser
from pydantic import (
    BaseModel,
    IPvAnyAddress,
    StrictStr,
    StrictInt,
    StrictBool,
    StrictFloat
)
from typing import (
    Optional, 
    Callable, 
    Literal,
    List,
    Union
)

from .request import Request


HTTPMethod = Literal[
    "GET",
    "HEAD",
    "OPTIONS",
    "POST",
    "PUT",
    "PATCH",
    "DELETE",
    "TRACE"
]


class Limit(BaseModel):
    max_requests: StrictInt
    min_requests: Optional[StrictInt]
    request_period: StrictStr='1s'
    reject_requests: StrictBool=True
    request_backoff: StrictStr='1s'
    cpu_limit: Optional[Union[StrictFloat, StrictInt]]
    memory_limit: Optional[StrictStr]
    limiter_type: Optional[
        Literal[
            "adaptive",
            "cpu-adaptive",
            "leaky-bucket",
            "rate-adaptive",
            "sliding-window",
            "token-bucket",
        ]
    ]
    limit_key: Optional[
        Callable[
            [
                Request,
                IPvAnyAddress,
            ],
            str
        ]
    ]
    rules: Optional[
        List[
            Callable[
                [
                    Request,
                    IPvAnyAddress,

                ],
                bool
            ]
        ]
    ]

    @property
    def backoff(self):
        return TimeParser(self.request_backoff).time

    @property
    def period(self):
        return TimeParser(self.request_period).time
    
    @property
    def memory(self):
        return MemoryParser(self.memory_limit).megabytes(accuracy=4)

    def get_key(
        self, 
        request: Request,
        ip_address: IPvAnyAddress,
        default: str='default'
    ):

        if self.limit_key is None:
            return default
        
        return self.limit_key(
            request,
            ip_address
        )

    def matches(
        self,
        request: Request,
        ip_address: IPvAnyAddress
    ):

        if self.rules is None:
            return True
        
        matches_rules = False
        
        for rule in self.rules:
            matches_rules = rule(
                request,
                ip_address
            )

        return matches_rules
