import asyncio
import functools
import psutil
import signal
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Union


def handle_loop_stop(
    signame,
    loop: asyncio.AbstractEventLoop,
    executor: ThreadPoolExecutor
):
    try:
        executor.shutdown()
        loop.close()

    except BrokenPipeError:
        pass

    except RuntimeError:
        pass


class BaseMonitor:

    def __init__(self) -> None:
        self.active: Dict[str, int] = {}
        self.collected: Dict[str, List[int]] = defaultdict(list)
        self.cpu_count = psutil.cpu_count()

        self._background_monitors: Dict[str, asyncio.Task] = {}
        self._running_monitors: Dict[str, bool] = {}

        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self._executor: Union[ThreadPoolExecutor, None] = None

    async def start_background_monitor(
        self,
        monitor_name: str,
        interval_sec: Union[int, float]=1
    ):
        if self._loop is None:
            self._loop = asyncio.get_event_loop()

        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=psutil.cpu_count(logical=False)
            )

            for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
                self._loop.add_signal_handler(
                    getattr(signal, signame),
                    lambda signame=signame: handle_loop_stop(
                        signame,
                        self._loop,
                        self._executor
                    )
                )

        self._background_monitors[monitor_name] = self._loop.run_in_executor(
            self._executor,
            functools.partial(
                self._monitor_at_interval,
                monitor_name,
                interval_sec=interval_sec
            )
        )

    def _monitor_at_interval(
        self, 
        monitor_name: str,
        interval_sec: Union[int, float]=1
    ):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        loop.run_until_complete(
            self._update_background_monitor(
                monitor_name,
                interval_sec=interval_sec
            )
        )
        
    async def _update_background_monitor(
        self,
        monitor_name: str,
        interval_sec: Union[int, float]=1
    ):
        raise NotImplementedError('Monitor background update method must be implemented in non-base Monitor class.')

    async def stop_background_monitor(
        self,
        monitor_name: str
    ):
        self._running_monitors[monitor_name] = False

        await self._background_monitors[monitor_name]

        if self.active.get(monitor_name):
            self.collected[monitor_name].extend(
                list(self.active[monitor_name])
            )

            del self.active[monitor_name]

    async def stop_all_background_monitors(self):

        for monitor_name in self._running_monitors.keys():
            self._running_monitors[monitor_name] = False

        await asyncio.gather(list(self._background_monitors.values()))

        for monitor_name in self._running_monitors.keys():
            self.collected[monitor_name] = list(self.active[monitor_name])
            del self.active[monitor_name]


    def close(self):
        if self._executor:
            self._executor.shutdown()