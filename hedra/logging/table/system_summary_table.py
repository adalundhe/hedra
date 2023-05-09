import plotille
from collections import (
    OrderedDict,
    defaultdict
)
from tabulate import tabulate
from typing import (
    List, 
    Union
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

        self.enabled_tables = {
            'system': False
        }

    def generate_tables(self):
        self.cpu_table = self._to_cpu_table()
        self.memory_table = self._to_memory_table()

    def show_tables(self):
        
        if any(self.enabled_tables.values()):
            self.logger.console.sync.info('\n-- System Metrics --')

        if self.enabled_tables.get('system'):
            self.logger.console.sync.info('\nCPU (% per core):\n')
            self.logger.console.sync.info(f'''{self.cpu_table}\n''')

            self.logger.console.sync.info('\nMemory (gb):\n')
            self.logger.console.sync.info(f'''{self.memory_table}\n''')

    def _to_cpu_table(self):
            table_rows: List[OrderedDict] = []

            for metrics_set in self.system_metrics_summaries:
                for metric_group in metrics_set.cpu:

                    table_row = OrderedDict()

                    for row_name in SystemMetricsSet.metrics_table_keys:
                            table_row[row_name] = metric_group.record.get(row_name)

                    table_rows.append(table_row)

            return tabulate(
                list(sorted(
                    table_rows,
                    key=lambda row: row['stage']
                )),
                headers='keys',
                missingval='None',
                tablefmt="simple",
                floatfmt='.2f'
            )
    
    def _to_memory_table(self):
            table_rows: List[OrderedDict] = []

            for metrics_set in self.system_metrics_summaries:
                for stage_metrics in metrics_set.memory.metrics.values():
                    for metric_group in stage_metrics.values():

                        table_row = OrderedDict()

                        for row_name in SystemMetricsSet.metrics_table_keys:
                             table_row[row_name] = metric_group.record.get(row_name)

                        table_rows.append(table_row)

            return tabulate(
                list(sorted(
                    table_rows,
                    key=lambda row: row['stage']
                )),
                headers='keys',
                missingval='None',
                tablefmt="simple",
                floatfmt='.2f'
            )

