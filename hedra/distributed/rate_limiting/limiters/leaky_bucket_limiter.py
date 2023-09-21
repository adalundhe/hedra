import asyncio
from hedra.distributed.models.http import Limit
from .base_limiter import BaseLimiter


class LeakyBucketLimiter(BaseLimiter):

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

        self._level = 0.0
        self._last_check = 0.0

    def _leak(self) -> None:

        if self._level:
            elapsed = self._loop.time() - self._last_check
            decrement = elapsed * self._rate_per_sec
            self._level = max(self._level - decrement, 0)            

        self._last_check = self._loop.time()

    def has_capacity(self, amount: float = 1) -> bool:

        self._leak()
        requested = self._level + amount

        if requested < self.max_rate:

            for fut in self._waiters.values():
                if not fut.done():
                    fut.set_result(True)
                    break

        return requested <= self.max_rate
