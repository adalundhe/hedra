import asyncio
import math
import statistics
from hedra.distributed.models.http import Limit
from .base_limiter import BaseLimiter

class AdaptiveRateLimiter(BaseLimiter):

    __slots__ = (
        "max_rate",
        "min_rate",
        "time_period",
        "history",
        "rate_history",
        "moments",
        "waiting",
        "last_request_time",
        "current_rate",
        "_rate_per_sec",
        "_level",
        "_waiters",
        "_loop",
        "_current_time",
        "_previous_count",
        "_last_slope",
        "_current_slope"
    )

    def __init__(
        self, 
        limit: Limit
    ):
        super().__init__(
            limit.max_requests,
            limit.period
        )

        min_requests = limit.min_requests
        if min_requests is None:
            min_requests = math.ceil(self.max_rate * 0.1)

        self.initial_rate = math.ceil(
            (self.max_rate - min_requests)/2
        )

        self.min_rate = min_requests

        self.history = []
        self.rate_history = []
        self.moments = []
        self.waiting = []


        self._loop = asyncio.get_event_loop()

        self._current_time = self._loop.time()
        self._previous_count = limit.max_requests

        self.last_request_time = self._loop.time()
        self.current_rate = self.initial_rate

    def get_next_rate(self):

        current_time = self._loop.time()

        elapsed_time = current_time - self.last_request_time
        self.history.append(elapsed_time)

        if len(self.history) > self.time_period:
            self.history.pop(0)

        average_time = statistics.mean(self.history)

        if average_time > 1 / self.current_rate:
            self.current_rate = max(self.min_rate, self.current_rate / 2)
        else:
            self.current_rate = min(self.max_rate, self.current_rate * 2)

        self.last_request_time = current_time

        return self.current_rate

    def has_capacity(self, amount: float = 1) -> bool:

        expected_rate = self.get_next_rate()

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

        if self._rate_per_sec < expected_rate:

            for fut in self._waiters.values():
                if not fut.done():
                    fut.set_result(True)
                    break

        return self._rate_per_sec <= expected_rate
