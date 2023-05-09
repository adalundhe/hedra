import uuid
import numpy
import statistics
from collections import defaultdict
from typing import Dict, List, Union
from .system_metrics_group import SystemMetricsGroup
from .system_metrics_set_types import (
    MonitorGroup,
    MemoryMonitorGroup,
    CPUMonitorGroup,
    SystemMetricsCollection,
    SessionMetricsCollection,
    SystemMetricGroupType
)


class SystemMetricsSet:

    metrics_header_keys = [
        'stage',
        'name',
        'group',
        'mean',
        'median',
        'max',
        'min',
        'stdev',
        'variance',
        'quantile_10th',
        'quantile_20th',
        'quantile_25th',
        'quantile_30th',
        'quantile_40th',
        'quantile_50th',
        'quantile_60th',
        'quantile_70th',
        'quantile_75th',
        'quantile_80th',
        'quantile_90th',
        'quantile_95th',
        'quantile_99th',
    ]

    def __init__(self, metrics: MonitorGroup) -> None:
        self.system_metrics_set_id = uuid.uuid4()
        self.system_cpu_metrics: Dict[str, List[Union[int, float]]] = defaultdict(list)
        self.system_memory_metrics: Dict[str, List[Union[int, float]]] = defaultdict(list)

        self.session_cpu_metrics: Dict[str, SystemMetricsCollection] = {}
        self.session_memory_metrics: Dict[str, SystemMetricsCollection] = {}

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

        self.metrics = metrics

        self.cpu_metrics_by_stage: Dict[str, CPUMonitorGroup] = {
            stage_name: stage_metrics.get(
                'cpu'
            ) for stage_name, stage_metrics in metrics.items()
        }

        self.memory_metrics_by_stage: Dict[str, MemoryMonitorGroup] = {
            stage_name: stage_metrics.get(
                'memory'
            ) for stage_name, stage_metrics in metrics.items()
        }

        self.cpu = SystemMetricsGroup(
            self.cpu_metrics_by_stage,
            SystemMetricGroupType.CPU
        )

        self.memory = SystemMetricsGroup(
            self.memory_metrics_by_stage,
            SystemMetricGroupType.MEMORY
        )

    def generate_system_summaries(self):
        self.cpu.aggregate()
        self.memory.aggregate()

        for stage_metrics in self.metrics.values():
            cpu_metrics_group = stage_metrics.get('cpu')

            for monitor_name, monitor_metrics in cpu_metrics_group.collected.items():
                self.system_cpu_metrics[monitor_name].extend(monitor_metrics)

        for stage_metrics in self.metrics.values():
            memory_metrics_group = stage_metrics.get('memory')

            for monitor_name, monitor_metrics in memory_metrics_group.collected.items():
                self.system_memory_metrics[monitor_name].extend(monitor_metrics)

        for monitor_name, monitor_metrics in self.system_cpu_metrics.items():
            self.session_cpu_metrics[monitor_name] = SystemMetricsCollection(**{
                'name': monitor_name,
                'group': SystemMetricGroupType.CPU.value,
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

        for monitor_name, monitor_metrics in self.system_memory_metrics.items():
            self.session_memory_metrics[monitor_name] = SystemMetricsCollection(**{
                'name': monitor_name,
                'group': SystemMetricGroupType.MEMORY.value,
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
