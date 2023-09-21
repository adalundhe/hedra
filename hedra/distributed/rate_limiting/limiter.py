from hedra.distributed.env import Env
from hedra.distributed.models.http import (
    Limit,
    Request
)
from pydantic import IPvAnyAddress
from typing import (
    Optional,
    Callable,
    Dict,
    Union
)
from .limiters import (
    AdaptiveRateLimiter,
    CPUAdaptiveLimiter,
    LeakyBucketLimiter,
    ResourceAdaptiveLimiter,
    SlidingWindowLimiter,
    TokenBucketLimiter
)

class Limiter:

    def __init__(
        self,
        env: Env
    ) -> None:
        
        self._limiter: Union[
            Union[
                AdaptiveRateLimiter,
                CPUAdaptiveLimiter,
                LeakyBucketLimiter,
                ResourceAdaptiveLimiter,
                SlidingWindowLimiter,
                TokenBucketLimiter
            ], 
            None
        ] = None

        self._default_limit = Limit(
            max_requests=env.MERCURY_SYNC_HTTP_RATE_LIMIT_REQUESTS,
            request_period=env.MERCURY_SYNC_HTTP_RATE_LIMIT_PERIOD,
            reject_requests=env.MERCURY_SYNC_HTTP_RATE_LIMIT_DEFAULT_REJECT,
            cpu_limit=env.MERCURY_SYNC_HTTP_CPU_LIMIT,
            memory_limit=env.MERCURY_SYNC_HTTP_MEMORY_LIMIT
        )
        
        self._rate_limit_strategy = env.MERCURY_SYNC_HTTP_RATE_LIMIT_STRATEGY
        self._default_limiter_type = env.MERCURY_SYNC_HTTP_RATE_LIMITER_TYPE
        
        self._rate_limiter_types: Dict[
            str,
            Callable[
                [Limit],
                Union[
                    AdaptiveRateLimiter,
                    CPUAdaptiveLimiter,
                    LeakyBucketLimiter,
                    ResourceAdaptiveLimiter,
                    SlidingWindowLimiter,
                    TokenBucketLimiter
                ]
            ]
        ] = {
            "adaptive": AdaptiveRateLimiter,
            "cpu-adaptive": CPUAdaptiveLimiter,
            "leaky-bucket": LeakyBucketLimiter,
            "rate-adaptive": ResourceAdaptiveLimiter,
            "sliding-window":  SlidingWindowLimiter,
            "token-bucket":  TokenBucketLimiter,
        }

        self._rate_limit_period = env.MERCURY_SYNC_HTTP_RATE_LIMIT_PERIOD

        self._rate_limiters: Dict[
            str, 
            Union[
                AdaptiveRateLimiter,
                CPUAdaptiveLimiter,
                LeakyBucketLimiter,
                SlidingWindowLimiter,
                TokenBucketLimiter
            ]
        ] = {}

    async def limit(
        self,
        ip_address: IPvAnyAddress,
        request: Request,
        limit: Optional[Limit]=None
    ):
        limit_key: Union[str, None] = None

        if self._rate_limit_strategy == 'ip':

            if limit is None:
                limit = self._default_limit

            limit_key = limit.get_key(
                request,
                ip_address,
                default=ip_address
            )

        elif self._rate_limit_strategy == 'endpoint' and limit:

            if limit is None:
                limit = self._default_limit

            limit_key = limit.get_key(
                request,
                ip_address,
                default=request.path
            )
        
        elif self._rate_limit_strategy == 'global':

            limit_key = self._default_limit.get_key(
                request,
                ip_address,
                default='default'
            )

            limit = self._default_limit

        elif self._rate_limit_strategy == "ip-endpoint" and limit:

            if limit is None:
                limit = self._default_limit

            limit_key = limit.get_key(
                request,
                ip_address,
                default=f'{request.path}_{ip_address}'
            )

        elif limit:

            limit_key = limit.get_key(
                request,
                ip_address
            )

        if limit_key and limit.matches(request, ip_address):
            return await self._check_limiter(
                limit_key,
                limit
            )

        return False 

    async def _check_limiter(
        self,
        limiter_key: str,
        limit: Limit
    ):
        
        limiter = self._rate_limiters.get(limiter_key)

        rate_limiter_type = limit.limiter_type
        if rate_limiter_type is None:
            rate_limiter_type = self._default_limiter_type

        if limiter is None:

            limiter = self._rate_limiter_types.get(rate_limiter_type)(limit)

            self._rate_limiters[limiter_key] = limiter
            

        return await limiter.acquire()
    
    async def close(self):
        for limiter in self._rate_limiters.values():
            if isinstance(limiter, CPUAdaptiveLimiter):
                await limiter.close()