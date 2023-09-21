import asyncio
import math
import os
import psutil
import statistics
from hedra.distributed.models.http import Limit
from typing import Union, List
from .base_limiter import BaseLimiter


class ResourceAdaptiveLimiter(BaseLimiter):

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
        "_cpu_history",
        "_memory_history",
        "_memory_limit",
        "_current_memory"
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

        self._memory_limit = limit.memory

        self._cpu_history: List[float] = []
        self._memory_history: List[float] = []

        self._max_queue = limit.max_requests
        self._sample_task: Union[asyncio.Task, None] = None
        self._running = False
        self._activate_limit = False
        self._process = psutil.Process(os.getpid())

        self._current_cpu = self._process.cpu_percent()
        self._current_memory = self._get_memory()

        self._cpu_history.append(self._current_cpu)
    
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

        while self._activate_limit:

            fut = self._loop.create_future()
            try:

                self._waiters[task] = fut

                await asyncio.wait_for(
                    asyncio.shield(fut), 
                    timeout=self._backoff
                )

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
            self._current_memory = self._get_memory()

            self._cpu_history.append(self._current_cpu)
            self._memory_history.append(self._current_memory)

            elapsed = self._loop.time() - self._last_check

            if elapsed > self.time_period:
                self._cpu_history.pop(0)


            median_cpu_usage = statistics.median(self._cpu_history)
            median_memory_usage = statistics.median(self._memory_history)


            if self._current_cpu >= self._cpu_limit or self._current_memory >= self._memory_limit:
                self._activate_limit = True

            elif median_cpu_usage < self._cpu_limit and median_memory_usage < self._memory_limit:
                self._activate_limit = False
                self._max_fast_backoff = max(
                    self._max_fast_backoff - (1/self._rate_per_sec),
                    self._min_backoff
                )

            
            await asyncio.sleep(0.1)

    def _get_memory(self):
        return self._process.memory_info().rss / 1024 ** 2

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