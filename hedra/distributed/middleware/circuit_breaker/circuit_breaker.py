import math
import asyncio
import random
from hedra.distributed.env import Env, load_env
from hedra.distributed.env.time_parser import TimeParser
from hedra.distributed.middleware.base import (
    Middleware,
    MiddlewareType
)
from hedra.distributed.middleware.base.types import RequestHandler
from hedra.distributed.models.http import (
    Request,
    Response
)
from hedra.distributed.rate_limiting.limiters import SlidingWindowLimiter
from typing import (
    Optional,
    Union
)
from .circuit_breaker_state import CircuitBreakerState


class CircuitBreaker(Middleware):

    def __init__(
        self,
        failure_threshold: Optional[float]=None,
        failure_window: Optional[str]=None,
        handler_timeout: Optional[str]=None,
        rejection_sensitivity: Optional[float]=None
    ) -> None:
        super().__init__(
            self.__class__.__name__,
            middleware_type=MiddlewareType.CALL
        )

        env = load_env(Env)

        if failure_threshold is None:
            failure_threshold = env.MERCURY_SYNC_HTTP_CIRCUIT_BREAKER_FAILURE_THRESHOLD
        
        if failure_window is None:
            failure_window = env.MERCURY_SYNC_HTTP_CIRCUIT_BREAKER_FAILURE_WINDOW

        if handler_timeout is None:
            handler_timeout = env.MERCURY_SYNC_HTTP_HANDLER_TIMEOUT

        if rejection_sensitivity is None:
            rejection_sensitivity = env.MERCURY_SYNC_HTTP_CIRCUIT_BREAKER_REJECTION_SENSITIVITY

        self.failure_threshold = failure_threshold
        self.rejection_sensitivity = rejection_sensitivity

        self.failure_window = TimeParser(failure_window).time
        self.handler_timeout = TimeParser(handler_timeout).time
        self._limiter_failure_window = failure_window

        self.overload = 0
        self.failed = 0
        self.succeeded = 0
        self.total_completed = 0

        self._rate_per_sec = 0
        self._rate_per_sec_succeeded = 0
        self._rate_per_sec_failed = 0

        self._previous_count = 0
        self._previous_count_succeeded = 0
        self._previous_count_failed = 0

        self.wraps: bool = False

        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self._current_time: Union[float, None]=None
        self._breaker_state = CircuitBreakerState.CLOSED

        self._limiter: Union[SlidingWindowLimiter, None]=None

        self._closed_window_start: Union[float, None] = None
        self._closed_elapsed = 0

        self._half_open_window_start: Union[float, None] = None
        self._half_open_elapsed = 0

    def trip_breaker(self) -> bool:

        failed_rate_threshold = max(
            self._rate_per_sec * self.failure_threshold,
            1
        )

        return int(self._rate_per_sec_failed) > int(failed_rate_threshold)
    
    def reject_request(self) -> bool:

        if (self._loop.time() - self._current_time) > self.failure_window:
            self._current_time = math.floor(
                self._loop.time()/self.failure_window
            ) * self.failure_window

            self._previous_count = self.total_completed
            self._previous_count_succeeded = self.succeeded
            self._previous_count_failed = self.failed

            self.failed = 0
            self.succeeded = 0
            self.total_completed = 0

        self._rate_per_sec = (
            self._previous_count * (
                self.failure_window - (self._loop.time() - self._current_time)
            )/self.failure_window
        ) + self.total_completed


        self._rate_per_sec_succeeded = (
            self._previous_count_succeeded * (
                self.failure_window - (self._loop.time() - self._current_time)
            )/self.failure_window
        ) + self.succeeded

        self._rate_per_sec_failed = (
            self._previous_count_failed * (
                self.failure_window - (self._loop.time() - self._current_time)
            )/self.failure_window
        ) + self.failed

        success_rate = self._rate_per_sec_succeeded/(1 - self.failure_threshold)

        rejection_probability = max(
            (self._rate_per_sec - success_rate)/(self._rate_per_sec + 1),
            0
        )**(1/self.rejection_sensitivity)
        
        return random.random() < rejection_probability

    async def __setup__(self):
        self._loop = asyncio.get_event_loop()
        self._current_time = self._loop.time()

    async def __run__(
        self,
        request: Request,
        handler: RequestHandler
    ):
        
        reject = self.reject_request()

        if self._breaker_state == CircuitBreakerState.OPEN and self._closed_elapsed < self.failure_window:
            self._closed_elapsed = self._loop.time() - self._closed_window_start
            reject = True

        elif self._breaker_state == CircuitBreakerState.OPEN:

            self._breaker_state = CircuitBreakerState.HALF_OPEN

            self._half_open_window_start = self._loop.time()
            self._closed_elapsed = 0

        if self._breaker_state == CircuitBreakerState.HALF_OPEN and self._half_open_elapsed < self.failure_window:
            self._half_open_elapsed = self._loop.time() - self._half_open_window_start

        elif self._breaker_state == CircuitBreakerState.HALF_OPEN:
            self._breaker_state = CircuitBreakerState.CLOSED
            self._half_open_elapsed = 0

        if reject:
            response = Response(
                request.path,
                request.method,
                headers={
                    'x-mercury-sync-overload': True
                }
            )

            status = 503
        
        else:

            try:

                response, status = await asyncio.wait_for(
                    handler(request),
                    timeout=self.handler_timeout
                )


                if self.wraps is False:
                    response = Response(
                        request.path,
                        request.method,
                        headers=handler.response_headers,
                        data=response
                    )

                
            except Exception:

                response = Response(
                    request.path,
                    request.method
                )

                status = 504
            
            # Don't count rejections toward failure stats.
            if status >= 400:
                self.failed += 1

            elif status < 400:
                self.succeeded += 1
            
            self.total_completed += 1

        breaker_open = self._breaker_state == CircuitBreakerState.CLOSED or self._breaker_state == CircuitBreakerState.HALF_OPEN

        if self.trip_breaker() and breaker_open:

            self._breaker_state = CircuitBreakerState.OPEN
            reject = True

            self._closed_window_start = self._loop.time()
            self._half_open_elapsed = 0

        return (
            request,
            response,
            status
        )
            
            
            


        

        
