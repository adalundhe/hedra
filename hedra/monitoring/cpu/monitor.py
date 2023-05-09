import psutil
from hedra.monitoring.base.monitor import BaseMonitor


class CPUMonitor(BaseMonitor):

    def __init__(self) -> None:
        super().__init__()

    def update_monitor(self, monitor_name: str):
        self.active[monitor_name].append(
            psutil.cpu_percent()
        )

    

        