import asyncio
import functools
import psutil
import signal
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from hedra.monitoring.base.monitor import BaseMonitor
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


class CPUMonitor(BaseMonitor):

    def __init__(self) -> None:
        super().__init__()
        self.active: Dict[str, List[int]] = defaultdict(list)

    def update_monitor(self, monitor_name: str):
        self.active[monitor_name].append(
            psutil.cpu_percent()
        )

    def store_monitor(self, monitor_name: str):
        self.collected[monitor_name] = list(self.active[monitor_name])
        del self.active[monitor_name]
        
    async def _update_background_monitor(
        self,
        monitor_name: str,
        interval_sec: Union[int, float]=1
    ):
        while self._running_monitors.get(monitor_name):
            self.active[monitor_name].append(
                psutil.cpu_percent()
            )

            await asyncio.sleep(interval_sec)


        