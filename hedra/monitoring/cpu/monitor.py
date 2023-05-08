import psutil
from collections import defaultdict
from hedra.monitoring.base.monitor import BaseMonitor
from typing import Dict, List, Union


class CPUMonitor(BaseMonitor):

    def __init__(self) -> None:
        super().__init__()
        self.active: Dict[str, Union[List[int], None]] = {}
        self.initial_usage: Dict[str, Union[int, None]] = {}
        self.collected_initial_usage: Dict[str, Union[int, None]] = {}

    def update_monitor(self, monitor_name: str):

        if self.active.get(monitor_name) is None:
            self.active[monitor_name] = []
            self.initial_usage[monitor_name] = psutil.cpu_percent()
        
        else:
            self.active[monitor_name].append(
                psutil.cpu_percent()
            )

    def store_monitor(self, monitor_name: str):
        self.collected[monitor_name] = list(self.active[monitor_name])
        self.collected_initial_usage[monitor_name] = self.initial_usage[monitor_name]
        
        del self.active[monitor_name]
        del self.initial_usage[monitor_name]