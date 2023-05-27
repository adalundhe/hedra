import statistics
import numpy
from collections import defaultdict
from typing import Dict, Union
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

        self.visibility_filters: Dict[str, Dict[str, bool]] = defaultdict(dict)

    def __iter__(self):
        for stage_metrics in self.metrics.values():
            for monitor_metrics in stage_metrics.values():
                yield monitor_metrics

    def aggregate(self):

        for stage_name, metrics in self.raw_metrics.items():
             for monitor_name, monitor_metrics in metrics.collected.items():

                self.visibility_filters[stage_name][monitor_name] = metrics.visibility_filters[monitor_name]

                if self.metrics_group == SystemMetricGroupType.MEMORY:
                    metrics_data = [
                        round(
                            metric_value/(1024**3),
                            2
                        ) for metric_value in monitor_metrics
                    ]

                else:
                    metrics_data = monitor_metrics

                if len(metrics_data) > 0:

                    self.stage_metrics[stage_name][monitor_name] = metrics.stage_metrics[monitor_name]

                    self.metrics[stage_name][monitor_name] = SystemMetricsCollection(**{
                        'stage': stage_name,
                        'name': monitor_name,
                        'group': self.metrics_group.value,
                        'mean': statistics.mean(metrics_data),
                        'median': statistics.median(metrics_data),
                        'max': max(metrics_data),
                        'min': min(metrics_data),
                        'stdev': statistics.stdev(metrics_data),
                        'variance': statistics.variance(metrics_data),
                        **{
                            f'quantile_{quantile}th':  numpy.quantile(
                                metrics_data,
                                round(
                                    quantile/100,
                                    2
                                )
                            ) for quantile in self._quantiles
                        }
                    })
