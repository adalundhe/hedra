import math
from hedra.distributed.models.http import Limit
from .base_limiter import BaseLimiter


class SlidingWindowLimiter(BaseLimiter):

    __slots__ = (
        "max_rate",
        "time_period",
        "_rate_per_sec",
        "_level",
        "_waiters",
        "_loop",
        "_current_time",
        "_previous_count"
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

        self._current_time = self._loop.time()
        self._previous_count = limit.max_requests

    def has_capacity(self, amount: float = 1) -> bool:

        if (self._loop.time() - self._current_time) > self.time_period:
            self._current_time = math.floor(
                self._loop.time()/self.time_period
            ) * self.time_period

            self._previous_count = self._level
            self._level = 0

        self._rate_per_sec = (
            self._previous_count * (
                self.time_period - (self._loop.time() - self._current_time)
            )/self.time_period
        ) + (self._level + amount)

        if self._rate_per_sec < self.max_rate:

            for fut in self._waiters.values():
                if not fut.done():
                    fut.set_result(True)
                    break

        return self._rate_per_sec <= self.max_rate