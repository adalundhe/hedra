import asyncio
import os
import psutil
from hedra.monitoring.base.monitor import BaseMonitor
from typing import Union





class MemoryMonitor(BaseMonitor):

    def __init__(self) -> None:
        super().__init__()

    def get_process_memory(self):
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        return mem_info.rss
    
    # decorator function
    def profile(self, func):
        def wrapper(*args, **kwargs):
    
            mem_before = self.get_process_memory()
            result = func(*args, **kwargs)
            mem_after = self.get_process_memory()
            
            self.collected[func.__name__].append(mem_after - mem_before)
    
            return result
        return wrapper
    
    def start_profile(self, monitor_name: str):
        self.active[monitor_name] = self.get_process_memory()

    def stop_profile(self, monitor_name: str):
        memory_after = self.get_process_memory()
        self.collected[monitor_name].append(
            memory_after - self.active.get(monitor_name, 0)
        )

        del self.active[monitor_name]

    def store_profile(self, monitor_name: str, value: Union[int, float]):
        self.collected[monitor_name].append(value)

    def rename_stored_profile(self, monitor_name: str, new_monitor_name: str):
        self.collected[new_monitor_name] = list(self.collected[monitor_name])
        del self.collected[monitor_name]
        
    async def _update_background_monitor(
        self,
        monitor_name: str,
        interval_sec: Union[int, float]=1
    ):
        while self._running_monitors.get(monitor_name):
            self.start_profile(monitor_name)
            await asyncio.sleep(interval_sec)
            self.stop_profile(monitor_name)

    