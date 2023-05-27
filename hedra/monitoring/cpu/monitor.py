import psutil
import itertools
import statistics
from hedra.monitoring.base.monitor import BaseMonitor
from typing import Optional


class CPUMonitor(BaseMonitor):

    def __init__(self) -> None:
        super().__init__()

    def update_monitor(self, monitor_name: str):
        self.active[monitor_name].append(
            psutil.cpu_percent()
        )

    def aggregate_worker_stats(self):
        
        monitor_stats = self._collect_worker_stats()

        for monitor_name, metrics in monitor_stats.items():
            self.stage_metrics[monitor_name] = [
                statistics.median(cpu_usage) for cpu_usage in itertools.zip_longest(
                    *metrics,
                    fillvalue=0
                )
            ]

        