import plotille
from collections import (
    OrderedDict,
    defaultdict
)
from termcolor import colored
from tabulate import tabulate
from typing import List, Union
from .table_types import ExecutionResults
from hedra.logging import HedraLogger


class ExecutionSummaryTable:

    def __init__(
        self,
        execution_results: ExecutionResults
    ) -> None:
        
        self.session_table: Union[str, None] = None
        self.stages_table: Union[str, None] = None
        self.stage_timings_table: Union[str, None] = None

        self.execution_results = execution_results
        self.session_table_rows: List[OrderedDict] = []

        self.stage_summary_tables = defaultdict(list)
        self.stage_streamed_data = defaultdict(dict)
        self._has_streamed = False
        self._graph_time_steps: List[int] = []

        self.session_metrics: List[str] = [
            'total',
            'succeeded',
            'failed'
        ]

        self.logger = HedraLogger()
        self.logger.initialize()

        self.enabled_tables = {
            'session': False,
            'stages': True
        }


    def generate_tables(self):
        self._generate_stage_and_session_tables()

        self.session_table = tabulate(
            self.session_table_rows,
            headers='keys',
            missingval='None',
            tablefmt="simple",
            floatfmt=".2f"
        )

        self.stages_table = tabulate(
            list(sorted(
                self.stage_summary_tables.get('stage_results'),
                key=lambda row: row['name']
            )),
            headers='keys',
            missingval='None',
            tablefmt="simple",
            floatfmt=(
                '.2f', 
                '.2f', 
                '.2f', 
                '.2f', 
                '.2f', 
                '.2f', 
                '.2f', 
                '.2f', 
                '.2E', 
                '.2E', 
                '.2E', 
                '.2E', 
                '.2E'
            )
        )

    def show_tables(self):

        self.logger.console.sync.info('')

        if self._has_streamed and self.enabled_tables.get('stages'):
            completion_rates = self.stage_streamed_data.get('completion_rates')
            for stage_name, stage_completion_rates in completion_rates.items():

                stage_summary = self.execution_results.get(stage_name)

                scatter_plot = plotille.scatter(
                    self._graph_time_steps,
                    stage_completion_rates,
                    width=120,
                    height=10,
                    y_min=0,
                    x_min=0,
                    x_max=int(round(
                        stage_summary.stage_metrics.time,
                        0
                    )),
                    linesep='\n',
                    X_label='time (sec)',
                    Y_label='completion rate',
                    lc='cyan',
                    marker='⨯'
                )

                self.logger.console.sync.info(f'''\n{stage_name} Completion Rates\n''')
                self.logger.console.sync.info(f'''{scatter_plot}\n''')
        
        if self.enabled_tables.get('session'):
            self.logger.console.sync.info('\nSession:\n')
            self.logger.console.sync.info(f'''{self.session_table}\n''')


        if self.enabled_tables.get('stages'):
            self.logger.console.sync.info('\nStages:\n')
            self.logger.console.sync.info(f'''{self.stages_table}\n''')

    def _generate_stage_and_session_tables(self):
        
        session_row = OrderedDict()

        for stage_summary in self.execution_results.values():

            stage_summary_dict = stage_summary.stage_metrics.dict()

            for field_name in self.session_metrics:
                if session_row.get(field_name) is None:
                    session_row[field_name] = stage_summary_dict.get(field_name)

                else:
                    session_row[field_name] += stage_summary_dict.get(field_name)

            table_row = OrderedDict()
            for field_name in stage_summary.stage_table_header_keys:
                header_name = stage_summary.stage_table_headers.get(field_name)
                table_row[header_name] = stage_summary_dict.get(field_name)

            self.stage_summary_tables['stage_results'].append(table_row)

            if stage_summary.stage_streamed_analytics:
                self._has_streamed = True

                time_steps = []
                current_batch_time = 0

                for time_step in stage_summary.stage_metrics.streamed_batch_timings:
                    current_batch_time += time_step
                    time_steps.append(current_batch_time)
                    
                self._graph_time_steps = time_steps

                self.stage_streamed_data['completed'][stage_summary.stage_metrics.name] = stage_summary.stage_metrics.streamed_completed
                self.stage_streamed_data['succeeded'][stage_summary.stage_metrics.name] = stage_summary.stage_metrics.streamed_succeeded
                self.stage_streamed_data['failed'][stage_summary.stage_metrics.name] = stage_summary.stage_metrics.streamed_failed
                self.stage_streamed_data['completion_rates'][stage_summary.stage_metrics.name] = stage_summary.stage_metrics.streamed_completion_rates

        self.session_table_rows.append(session_row)
            


