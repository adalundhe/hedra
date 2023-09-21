import asyncio
import math
import os
import psutil
import statistics
from hedra.distributed.models.http import Limit
from typing import Union, List
from .base_limiter import BaseLimiter


class CPUAdaptiveLimiter(BaseLimiter):

    __slots__ = (
        "max_rate",
        "time_period",
        "_rate_per_sec",
        "_level",
        "_waiters",
        "_loop",
        "_last_check",
        "_cpu_limit",
        "_current_time",
        "_previous_count",
        "_current_cpu",
        "_max_queue",
        "_sample_task",
        "_running",
        "_process",
        "_max_fast_backoff",
        "_min_backoff",
        "_history"
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

        cpu_limit = limit.cpu_limit
        if cpu_limit is None:
            cpu_limit = 50

        self._cpu_limit = cpu_limit
        self._backoff = limit.backoff
        self._min_backoff = self._backoff
        self._max_fast_backoff = math.ceil(self._backoff * 10)
        self._max_backoff = math.ceil(self._max_fast_backoff * 10)
        self._last_check = self._loop.time()
        self._current_time = self._loop.time()
        self._previous_count = limit.max_requests

        self._history: List[float] = []

        self._max_queue = limit.max_requests
        self._sample_task: Union[asyncio.Task, None] = None
        self._running = False
        self._activate_limit = False
        self._process = psutil.Process(os.getpid())

        self._current_cpu = self._process.cpu_percent()
        self._history.append(self._current_cpu)

    def has_capacity(self, amount: float = 1) -> bool:

        elapsed = self._loop.time() - self._last_check

        self._backoff = max(
            self._backoff - (1 / self._rate_per_sec * elapsed), 
            self._min_backoff
        )

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
        
        self._last_check = self._loop.time()

        return self._rate_per_sec <= self.max_rate
    
    async def acquire(
        self, 
        amount: float = 1,
    ):
        
        if not self._running:
            self._running = True
            self._sample_task = asyncio.create_task(
                self._sample_cpu()
            )
        
        if amount > self.max_rate:
            raise ValueError("Can't acquire more than the maximum capacity")


        task = asyncio.current_task(
            loop=self._loop
        )

        assert task is not None

        rejected = False

        while not self.has_capacity(amount) or self._activate_limit:

            fut = self._loop.create_future()
            try:

                self._waiters[task] = fut

                await asyncio.wait_for(
                    asyncio.shield(fut), 
                    timeout=self._backoff
                )

                if self._activate_limit:
                    await asyncio.sleep(self._backoff)
                    self._max_fast_backoff = min(
                        self._max_fast_backoff + (1/math.sqrt(self._rate_per_sec)),
                        self._max_backoff
                    )

            except asyncio.TimeoutError:
                pass
                
            fut.cancel()
            
            rejected = True

        self._backoff = min(self._backoff * 2, self._max_fast_backoff)
        self._waiters.pop(task, None)
        self._level += amount

        return rejected
    
    async def _sample_cpu(self):
        while  self._running:

            self._current_cpu = self._process.cpu_percent()
            self._history.append(self._current_cpu)

            elapsed = self._loop.time() - self._last_check

            if elapsed > self.time_period:
                self._history.pop(0)

            if self._current_cpu >= self._cpu_limit:
                self._activate_limit = True

            elif statistics.median(self._history) < self._cpu_limit:
                self._activate_limit = False
                self._max_fast_backoff = max(
                    self._max_fast_backoff - (1/self._rate_per_sec),
                    self._min_backoff
                )

            await asyncio.sleep(0.1)

    async def close(self):
        self._running = False

        self._sample_task.cancel()
        if not self._sample_task.cancelled():
            try:

                await self._sample_task

            except (
                asyncio.CancelledError,
                asyncio.InvalidStateError
            ):
                pass