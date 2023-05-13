import plotille
from collections import (
    OrderedDict,
    defaultdict
)
from tabulate import tabulate
from typing import (
    List, 
    Union,
    Dict
)
from hedra.reporting.system.system_metrics_set import SystemMetricsSet
from hedra.logging import HedraLogger


class SystemSummaryTable:

    def __init__(self) -> None:
        self.system_metrics_summaries: List[SystemMetricsSet] = []
        self.logger = HedraLogger()
        self.logger.initialize()

        self.cpu_table: Union[str, None] = None
        self.memory_table: Union[str, None] = None
        self.mb_per_vu_table: Union[str, None] = None
        self.graph_cpu_table: Union[str, None] = None
        self.graph_memory_table: Union[str, None] = None

        self.cpu_plot_rows: Dict[str, List[Union[int, float]]] = defaultdict(list)
        self.memory_plot_rows: Dict[str, List[Union[int, float]]] = defaultdict(list)
        self.graph_metrics_summary: Union[SystemMetricsSet, None] = None

        self.enabled_tables = {
            'system': False
        }

    def generate_tables(self):
        self.graph_cpu_table = self._to_graph_cpu_table()
        self.graph_memory_table = self._to_graph_memory_table()
        
        self.cpu_table = self._to_cpu_table()
        self.memory_table = self._to_memory_table()
        self.mb_per_vu_table = self._to_mb_per_vu_table()

    def show_tables(self):
        
        if any(self.enabled_tables.values()):
            self.logger.console.sync.info('\n-- System Metrics --')

        if self.enabled_tables.get('system'):
            for stage_name, monitor in self.graph_metrics_summary.cpu.stage_metrics.items():
                for monitor_name, metrics in monitor.items():

                    show_plot = self.graph_metrics_summary.cpu.visibility_filters[stage_name][monitor_name]

                    if show_plot:
                        scatter_plot = plotille.scatter(
                            [idx for idx in range(
                                1, 
                                len(metrics) + 1
                            )],
                            metrics,
                            width=120,
                            height=10,
                            y_min=0,
                            x_min=0,
                            x_max=len(metrics) + 1,
                            linesep='\n',
                            X_label='time (sec)',
                            Y_label='pct. utilization per thread',
                            lc='cyan',
                            marker='⨯'
                        )

                        self.logger.console.sync.info(f'''\n{monitor_name} % CPU Usage (Per Worker)\n''')
                        self.logger.console.sync.info(f'''{scatter_plot}\n''')

            for stage_name, monitor in self.graph_metrics_summary.memory.stage_metrics.items():
                for monitor_name, metrics in monitor.items():
                    
                    show_plot = self.graph_metrics_summary.memory.visibility_filters[stage_name][monitor_name]

                    if show_plot:
                        scatter_plot = plotille.scatter(
                            [idx for idx in range(
                                1, 
                                len(metrics) + 1
                            )],
                            [
                                round(
                                    metric_value/(1024**3),
                                    2
                                ) for metric_value in metrics
                            ],
                            width=120,
                            height=10,
                            y_min=0,
                            x_min=0,
                            x_max=len(metrics) + 1,
                            linesep='\n',
                            X_label='time (sec)',
                            Y_label='memory used (gb)',
                            lc='cyan',
                            marker='⨯'
                        )

                        self.logger.console.sync.info(f'''\n{monitor_name} % Memory Usage (gb)\n''')
                        self.logger.console.sync.info(f'''{scatter_plot}\n''')

            self.logger.console.sync.info('\nCPU (% per worker):\n')
            self.logger.console.sync.info(f'''{self.graph_cpu_table}\n''')

            self.logger.console.sync.info('\nMemory (gb):\n')
            self.logger.console.sync.info(f'''{self.graph_memory_table}\n''')

        if self.enabled_tables.get('system') and self.enabled_tables.get('stages'):
            
            seen_plots: List[str] = []

            for metrics_set in self.system_metrics_summaries:
                for stage_name, monitor in metrics_set.cpu.stage_metrics.items():
                    for monitor_name, metrics in monitor.items():

                        show_plot = metrics_set.cpu.visibility_filters[stage_name][monitor_name]

                        if show_plot and monitor_name not in seen_plots:
                            scatter_plot = plotille.scatter(
                                [idx for idx in range(
                                    1, 
                                    len(metrics) + 1
                                )],
                                metrics,
                                width=120,
                                height=10,
                                y_min=0,
                                x_min=0,
                                x_max=len(metrics) + 1,
                                linesep='\n',
                                X_label='time (sec)',
                                Y_label='pct. utilization per thread',
                                lc='cyan',
                                marker='⨯'
                            )

                            self.logger.console.sync.info(f'''\n{monitor_name} % CPU Usage (Per Worker)\n''')
                            self.logger.console.sync.info(f'''{scatter_plot}\n''')

                            seen_plots.append(monitor_name)

            seen_plots: List[str] = []

            for metrics_set in self.system_metrics_summaries:
                for stage_name, monitor in metrics_set.memory.stage_metrics.items():
                    for monitor_name, metrics in monitor.items():
                        
                        show_plot = metrics_set.cpu.visibility_filters[stage_name][monitor_name]

                        if show_plot and monitor_name not in seen_plots:
                            scatter_plot = plotille.scatter(
                                [idx for idx in range(
                                    1, 
                                    len(metrics) + 1
                                )],
                                [
                                    round(
                                        metric_value/(1024**3),
                                        2
                                    ) for metric_value in metrics
                                ],
                                width=120,
                                height=10,
                                y_min=0,
                                x_min=0,
                                x_max=len(metrics) + 1,
                                linesep='\n',
                                X_label='time (sec)',
                                Y_label='memory used (gb)',
                                lc='cyan',
                                marker='⨯'
                            )

                            self.logger.console.sync.info(f'''\n{monitor_name} % Memory Usage (gb)\n''')
                            self.logger.console.sync.info(f'''{scatter_plot}\n''')

                            seen_plots.append(monitor_name)

            self.logger.console.sync.info('\nCPU (% per worker):\n')
            self.logger.console.sync.info(f'''{self.cpu_table}\n''')

            self.logger.console.sync.info('\nMemory (gb):\n')
            self.logger.console.sync.info(f'''{self.memory_table}\n''')

            self.logger.console.sync.info('\nMemory per VU (mb):\n')
            self.logger.console.sync.info(f'''{self.mb_per_vu_table}\n''')

    def _to_graph_cpu_table(self):

        table_row = OrderedDict()
        for metric_group in self.graph_metrics_summary.cpu:
            for row_name in SystemMetricsSet.metrics_table_keys:
                    table_row[row_name] = metric_group.record.get(row_name)

        return tabulate(
            [table_row],
            headers='keys',
            missingval='None',
            tablefmt="simple",
            floatfmt='.2f'
        )
    
    def _to_graph_memory_table(self):

        table_row = OrderedDict()
        for metric_group in self.graph_metrics_summary.memory:
            for row_name in SystemMetricsSet.metrics_table_keys:
                    table_row[row_name] = metric_group.record.get(row_name)

        return tabulate(
            [table_row],
            headers='keys',
            missingval='None',
            tablefmt="simple",
            floatfmt='.2f'
        )

    def _to_cpu_table(self):
        
        seen_monitors: List[str] = []
        table_rows: List[OrderedDict] = []

        for metrics_set in self.system_metrics_summaries:
            for metric_group in metrics_set.cpu:
                
                if metric_group.name not in seen_monitors:

                    table_row = OrderedDict()

                    for row_name in SystemMetricsSet.metrics_table_keys:
                            table_row[row_name] = metric_group.record.get(row_name)

                    table_rows.append(table_row)
                    seen_monitors.append(metric_group.name)

        return tabulate(
            list(sorted(
                table_rows,
                key=lambda row: row['name']
            )),
            headers='keys',
            missingval='None',
            tablefmt="simple",
            floatfmt='.2f'
        )
    
    def _to_memory_table(self):

        seen_monitors: List[str] = []
        table_rows: List[OrderedDict] = []

        for metrics_set in self.system_metrics_summaries:
            for stage_metrics in metrics_set.memory.metrics.values():
                for metric_group in stage_metrics.values():
                    
                    if metric_group.name not in seen_monitors:

                        table_row = OrderedDict()

                        for row_name in SystemMetricsSet.metrics_table_keys:
                                table_row[row_name] = metric_group.record.get(row_name)

                        table_rows.append(table_row)
                        seen_monitors.append(metric_group.name)

        return tabulate(
            list(sorted(
                table_rows,
                key=lambda row: row['name']
            )),
            headers='keys',
            missingval='None',
            tablefmt="simple",
            floatfmt='.2f'
        )
    
    def _to_mb_per_vu_table(self):

        seen_monitors: List[str] = []
        table_rows: List[OrderedDict] = []

        for metrics_set in self.system_metrics_summaries:
            for monitor_name, stage_metrics in metrics_set.mb_per_vu.items():

                if monitor_name not in seen_monitors:
                        
                    table_row = OrderedDict()

                    for row_name in SystemMetricsSet.metrics_table_keys:
                        table_row[row_name] = stage_metrics.record.get(row_name)

                    table_rows.append(table_row)
                    seen_monitors.append(monitor_name)

        return tabulate(
            list(sorted(
                table_rows,
                key=lambda row: row['name']
            )),
            headers='keys',
            missingval='None',
            tablefmt="simple",
            floatfmt='.2f'
        )

