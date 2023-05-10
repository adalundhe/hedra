import os
import psutil
from hedra.monitoring.base.monitor import BaseMonitor





class MemoryMonitor(BaseMonitor):

    def __init__(self) -> None:
        super().__init__()
        self.total_memory = psutil.virtual_memory().total

    def update_monitor(self, monitor_name: str):
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()

        self.active[monitor_name].append(mem_info.rss)
