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

    metrics_table_keys = [
        'name',
        'mean',
        'median',
        'max',
        'min',
        'stdev',
        'variance'
    ]

    session_metrics_metadata = [
        'name',
        'group',
    ]

    stage_metrics_metadata = [
        'stage',
        'name',
        'group',
    ]

    primary_metrics = [
        'mean',
        'median',
        'max',
        'min',
        'stdev',
        'variance',
    ]

    quantiles = [
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
        'quantile_99th'
    ]

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
        'quantile_99th'
    ]

    def __init__(
            self, 
            metrics: MonitorGroup,
            batch_sizes: Dict[str, int]
        ) -> None:
        
        self.system_metrics_set_id = uuid.uuid4()
        self.system_cpu_metrics: Dict[str, List[Union[int, float]]] = defaultdict(list)
        self.system_memory_metrics: Dict[str, List[Union[int, float]]] = defaultdict(list)

        self.session_cpu_metrics: Dict[str, SystemMetricsCollection] = {}
        self.session_memory_metrics: Dict[str, SystemMetricsCollection] = {}
        self.mb_per_vu: Dict[str, SystemMetricsCollection] = {}
        self.batch_sizes = batch_sizes

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
        
        for stage_name, stage_metrics in self.metrics.items():
            memory_metrics_group = stage_metrics.get('memory')

            for monitor_name, monitor_metrics in memory_metrics_group.collected.items():
                self.system_memory_metrics[monitor_name].extend(monitor_metrics)

                is_visible = memory_metrics_group.visibility_filters[monitor_name]
                stage_batch_size = self.batch_sizes.get(stage_name)

                if memory_metrics_group.is_execute_stage and is_visible and stage_batch_size:


                    mb_per_vu = [
                        round(
                            memory_used/(1024**2 * stage_batch_size),
                            2
                        ) for memory_used in monitor_metrics
                    ]

                    self.mb_per_vu[stage_name] = SystemMetricsCollection(**{
                        'stage': stage_name,
                        'name': monitor_name,
                        'group': SystemMetricGroupType.MEMORY.value,
                        'mean': statistics.mean(mb_per_vu),
                        'median': statistics.median(mb_per_vu),
                        'max': max(mb_per_vu),
                        'min': min(mb_per_vu),
                        'stdev': statistics.stdev(mb_per_vu),
                        'variance': statistics.variance(mb_per_vu),
                        **{
                            f'quantile_{quantile}th':  numpy.quantile(
                                mb_per_vu,
                                round(
                                    quantile/100,
                                    2
                                )
                            ) for quantile in self._quantiles
                        }
                    })


        for monitor_name, monitor_metrics in self.system_cpu_metrics.items():
            self.session_cpu_metrics[monitor_name] = SessionMetricsCollection(**{
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
            self.session_memory_metrics[monitor_name] = SessionMetricsCollection(**{
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
