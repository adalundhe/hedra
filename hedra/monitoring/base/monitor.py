import asyncio
import functools
import psutil
import signal
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from .exceptions import MonitorKilledError
from typing import (
    Dict, 
    List, 
    Union,
    Any
)


WorkerMetrics = Dict[int, Dict[str, List[Union[int, float]]]] 


def handle_thread_loop_stop(
    loop: asyncio.AbstractEventLoop,
    monitor_name: str,
    running_monitors: Dict[str, bool]=None,

):
    running_monitors[monitor_name] = False
    loop.stop()
    loop.close()


def handle_monitor_stop(
    loop: asyncio.AbstractEventLoop=None,
    running_monitors: Dict[str, bool]=None,
    executor: ThreadPoolExecutor=None
):
    try:

        for monitor_name in running_monitors:
            running_monitors[monitor_name] = False

        executor.shutdown(wait=False, cancel_futures=True)

        if loop and loop.is_running():
            loop.stop()

        raise MonitorKilledError()
    
    except MonitorKilledError:
        executor.shutdown(wait=False, cancel_futures=True)
        pass

    except BrokenPipeError:
        executor.shutdown(wait=False, cancel_futures=True)
        raise MonitorKilledError()

    except RuntimeError:
        executor.shutdown(wait=False, cancel_futures=True)
        raise MonitorKilledError()
    
    finally:
        pass
    

class BaseMonitor:

    def __init__(self) -> None:
        self.active: Dict[str, List[int]] = defaultdict(list)
        self.collected: Dict[str, List[int]] = defaultdict(list)
        self.cpu_count = psutil.cpu_count()
        self.stage_metrics: Dict[str, List[Union[int, float]]] = {}
        self.visibility_filters: Dict[str, bool] = defaultdict(lambda: False)
        self.stage_type: Union[Any, None] = None
        self.worker_metrics: WorkerMetrics = defaultdict(dict)
        self.is_execute_stage = False

        self._background_monitors: Dict[str, asyncio.Task] = {}
        self._sync_background_monitors: Dict[str, asyncio.Future] = {}
        self._running_monitors: Dict[str, bool] = {}

        self._loop: Union[asyncio.AbstractEventLoop, None] = None
        self._executor: Union[ThreadPoolExecutor, None] = None

    def start_background_monitor_sync(
        self,
        monitor_name: str,
        interval_sec: Union[int, float]=1
    ):
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=psutil.cpu_count(logical=False)
            )

            for signame in ('SIGINT', 'SIGTERM', 'SIG_IGN'):
                signal_type: signal = getattr(signal, signame)

                signal.signal(
                    signal_type, 
                    lambda sig, frame: handle_monitor_stop(  
                        running_monitors=self._running_monitors,
                        executor=self._executor
                    )
                )
        
        self._sync_background_monitors[monitor_name] = self._executor.submit(
            functools.partial(
                self._monitor_at_interval,
                monitor_name,
                interval_sec=interval_sec
            )
        )

    def aggregate_worker_stats(self):
        raise NotImplementedError('Aggregate worker stats method method must be implemented in a non-base Monitor class.')

    def _collect_worker_stats(self):

        monitor_stats: Dict[str, List[Union[int, float]]] = defaultdict(list)

        for worker_id in self.worker_metrics:
            for monitor_name, metrics in self.worker_metrics[worker_id].items():
                monitor_stats[monitor_name].append(metrics)

        return monitor_stats

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
            signal_type: signal = getattr(signal, signame)

            signal.signal(
                signal_type, 
                lambda sig, frame: handle_monitor_stop(  
                    loop=self._loop,
                    running_monitors=self._running_monitors,
                    executor=self._executor
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

    def update_monitor(str, monitor_name: str) -> Union[int, float]:
        raise NotImplementedError('Monitor background update method must be implemented in non-base Monitor class.')

    def store_monitor(self, monitor_name: str):
        self.collected[monitor_name] = list(self.active[monitor_name])
        del self.active[monitor_name]

    def trim_monitor_samples(
        self,
        monitor_name: str,
        trim_length: int
    ):
        if self.collected.get(monitor_name):
            self.collected[monitor_name][:trim_length]
        
    async def _update_background_monitor(
        self,
        monitor_name: str,
        interval_sec: Union[int, float]=1
    ):
        while self._running_monitors.get(monitor_name):
            await asyncio.sleep(interval_sec)
            self.update_monitor(monitor_name)

    def _monitor_at_interval(
        self, 
        monitor_name: str,
        interval_sec: Union[int, float]=1
):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        self._running_monitors[monitor_name] = True

        try:
            loop.run_until_complete(
                self._update_background_monitor(
                    monitor_name,
                    interval_sec=interval_sec
                )
            )

        except Exception:
            self._running_monitors[monitor_name] = False
            raise RuntimeError()
        
    def stop_background_monitor_sync(
        self,
        monitor_name: str
    ):

        self._running_monitors[monitor_name] = False
        self._sync_background_monitors[monitor_name].result()

        if self.active.get(monitor_name):
            self.collected[monitor_name].extend(
                list(self.active[monitor_name])
            )

    async def stop_background_monitor(
        self,
        monitor_name: str
    ):
        self._running_monitors[monitor_name] = False

        if not self._background_monitors[monitor_name].cancelled():
            await self._background_monitors[monitor_name]

        if self.active.get(monitor_name):
            self.collected[monitor_name].extend(
                list(self.active[monitor_name])
            )

    def stop_all_background_monitors_sync(self):

        for monitor_name in self._running_monitors.keys():
            self._running_monitors[monitor_name] = False

        for monitor in self._sync_background_monitors.values():
            monitor.result()

        for monitor_name in self._running_monitors.keys():
            self.collected[monitor_name] = list(self.active[monitor_name])

    async def stop_all_background_monitors(self):

        for monitor_name in self._running_monitors.keys():
            self._running_monitors[monitor_name] = False

        await asyncio.gather(list(self._background_monitors.values()))

        for monitor_name in self._running_monitors.keys():
            self.collected[monitor_name] = list(self.active[monitor_name])

    def close(self):
        if self._executor:
            self._executor.shutdown(wait=False, cancel_futures=True)