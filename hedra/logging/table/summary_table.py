from typing import Dict, Union, List
from collections import defaultdict
from hedra.reporting.experiment.experiment_metrics_set import ExperimentSummary
from .execution_summary_table import ExecutionSummaryTable
from .experiments_summary_table import ExperimentsSummaryTable
from .table_types import (
    GraphExecutionResults,
    ExecutionResults
)


class SummaryTable:

    def __init__(
        self,
        graph_execution_results: GraphExecutionResults,
        summaries_visibility_config: Dict[str, bool]={}
    ) -> None:
        self.experiment_summary_table: Union[ExperimentsSummaryTable, None] = None
        self.execution_summary_table: Union[ExecutionSummaryTable, None] = None

        
        experiment_summaries: Dict[str, ExperimentSummary] = {}
        execution_results: ExecutionResults = {}

        experiment_headers: Dict[str, Dict[str]] = defaultdict(dict)
        experiment_headers_keys: Dict[str, List[str]] = defaultdict(list)

        for results_set in graph_execution_results.values():
            stage_execution_results: ExecutionResults = results_set.get('stages')
            execution_results.update(stage_execution_results)
            
            
            experiment_metrics_sets = results_set.get('experiment_metrics_sets', {})

            for experiment_name, experiment in experiment_metrics_sets.items():
                experiment_summaries[experiment_name] = experiment.experiments_summary

                experiment_headers['experiment_table_headers'].update(
                    experiment.experiments_table_headers
                )

                experiment_headers['variants_table_headers'].update(
                    experiment.variants_table_headers
                )

                experiment_headers['mutations_table_headers'].update(
                    experiment.mutations_table_headers
                )

                if len(experiment_headers_keys['experiments_table_headers_keys']) == 0:
                    experiment_headers_keys['experiments_table_headers_keys'].extend(
                        experiment.experiments_table_header_keys
                    )

                if len(experiment_headers_keys['variant_table_headers_keys']) == 0:
                    experiment_headers_keys['variant_table_headers_keys'].extend(
                        experiment.variants_table_header_keys
                    )

                if len(experiment_headers_keys['variants_stats_table_header_keys']) == 0:
                    experiment_headers_keys['variants_stats_table_header_keys'].extend(
                        experiment.variants_stats_table_header_keys
                    )

                if len(experiment_headers_keys['mutations_table_header_keys']) == 0:
                    experiment_headers_keys['mutations_table_header_keys'].extend(
                        experiment.mutations_table_headers_keys
                    )

        self.execution_summary_table = ExecutionSummaryTable(execution_results)
        self.execution_summary_table.enabled_tables.update({
            table_name: enabled for table_name, enabled in summaries_visibility_config.items() if table_name in self.execution_summary_table.enabled_tables
        })
        

        if len(experiment_summaries) > 0:
            self.experiment_summary_table = ExperimentsSummaryTable(
                experiment_summaries,
                experiment_headers,
                experiment_headers_keys
            )
            
            self.experiment_summary_table.enabled_tables.update({
                table_name: enabled for table_name, enabled in summaries_visibility_config.items() if table_name in self.experiment_summary_table.enabled_tables
            })

    def generate_tables(self):
        self.execution_summary_table.generate_tables()
        if self.experiment_summary_table:
            self.experiment_summary_table.generate_tables()

    def show_tables(self):
        self.execution_summary_table.show_tables()
        if self.experiment_summary_table:
            self.experiment_summary_table.show_tables()