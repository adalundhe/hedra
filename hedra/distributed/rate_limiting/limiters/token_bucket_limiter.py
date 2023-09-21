import asyncio
from hedra.distributed.models.http import (
    Limit,
    HTTPMessage,
    Request
)
from types import TracebackType
from typing import (
    Optional, 
    Type
)
from .base_limiter import BaseLimiter


class TokenBucketLimiter(BaseLimiter):

    __slots__ = (
        "max_rate",
        "time_period",
        "_rate_per_sec",
        "_level",
        "_waiters",
        "_loop",
        "_last_check"
    )

    def __init__(
        self, 
        limit: Limit
    ) -> None:
        super().__init__(
            limit.max_requests,
            limit.period,
            reject_requests=limit.reject_requests
        )

        self._level = limit.max_requests
        self._last_check = self._loop.time()

    def has_capacity(self, amount: float = 1) -> bool:

        if self._level < self.max_rate:
            current_time = self._loop.time()
            delta = self._rate_per_sec * (current_time - self._last_check)
            self._level = min(self.max_rate, self._level + delta)
            self._last_check = current_time

        requested_amount = self._level - amount
        if requested_amount > 0 or self._level >= self.max_rate:
            for fut in self._waiters.values():
                if not fut.done():
                    fut.set_result(True)
                    break


        return amount < self._level
    
    async def acquire(
        self, 
        amount: float = 1
    ):
        
        if amount > self.max_rate:
            raise ValueError("Can't acquire more than the maximum capacity")


        task = asyncio.current_task(
            loop=self._loop
        )

        assert task is not None

        rejected = False

        if not self.has_capacity(amount) and self._reject_requests:
            return True

        while not self.has_capacity(amount):

            fut = self._loop.create_future()

            try:

                self._waiters[task] = fut
                await asyncio.wait_for(
                    asyncio.shield(fut), 
                    timeout=(1 / self._rate_per_sec * amount)
                )

            except asyncio.TimeoutError:
                pass

            fut.cancel()
            if self._reject_requests:
                rejected = True

        self._waiters.pop(task, None)
        self._level -= amount

        return rejected

    async def reject(
        self,
        request: Request,
        transport: asyncio.Transport
    ):
        if transport.is_closing() is False:

            server_error_respnse = HTTPMessage(
                path=request.path,
                status=429,
                error='Too Many Requests',
                method=request.method
            )

            transport.write(server_error_respnse.prepare_response())


    async def __aenter__(self) -> None:
        await self.acquire()

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> None:
        return None