import psutil
from collections import defaultdict
from hedra.monitoring.base.monitor import BaseMonitor
from typing import Dict, List


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