import statistics
import numpy
from collections import defaultdict
from typing import List, Dict, Union
from .system_metrics_set_types import (
    CPUMonitorGroup,
    MemoryMonitorGroup,
    StageSystemMetricsGroup,
    SystemMetricsCollection,
    SystemMetricGroupType
)


class SystemMetricsGroup:

    def __init__(
            self, 
            metrics: Union[CPUMonitorGroup, MemoryMonitorGroup],
            metric_group: SystemMetricGroupType
        ) -> None:
        self.stage_metrics: StageSystemMetricsGroup = defaultdict(dict)

        self.raw_metrics: Union[CPUMonitorGroup, MemoryMonitorGroup] = metrics
        self.metrics_group = metric_group
        self.metrics: Dict[str, Dict[str, SystemMetricsCollection]] = defaultdict(dict)
        self._quantiles = [
            10,
            20,
            25,
            30,
            40,
            50,
            60,
            70,
            75,
            80,
            90,
            95,
            99
        ]

    def __iter__(self):
        for stage_metrics in self.metrics.values():
            for monitor_metrics in stage_metrics.values():
                yield monitor_metrics

    def aggregate(self):

        for stage_name, metrics in self.raw_metrics.items():
             for monitor_name, monitor_metrics in metrics.collected.items():

                self.stage_metrics[stage_name][monitor_name] = metrics.stage_metrics[monitor_name]

                self.metrics[stage_name][monitor_name] = SystemMetricsCollection(**{
                    'stage': stage_name,
                    'name': monitor_name,
                    'group': self.metrics_group.value,
                    'mean': statistics.mean(monitor_metrics),
                    'median': statistics.median(monitor_metrics),
                    'max': max(monitor_metrics),
                    'min': min(monitor_metrics),
                    'stdev': statistics.stdev(monitor_metrics),
                    'variance': statistics.variance(monitor_metrics),
                    **{
                        f'quantile_{quantile}th':  numpy.quantile(
                            monitor_metrics,
                            round(
                                quantile/100,
                                2
                            )
                        ) for quantile in self._quantiles
                    }
                })
