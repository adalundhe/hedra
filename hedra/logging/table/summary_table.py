from typing import Dict, Union, List
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
        graph_execution_results: GraphExecutionResults
    ) -> None:
        pass
        self.experiment_summary_table: Union[ExperimentsSummaryTable, None] = None
        self.execution_summary_table: Union[ExecutionSummaryTable, None] = None

        
        experiment_summaries: Dict[str, ExperimentSummary] = {}
        execution_results: ExecutionResults = {}

        experiments_table_headers: Dict[str, str] = {}
        variants_table_headers: Dict[str, str] = {}
        mutations_table_headers: Dict[str, str] = {}

        experiments_table_headers_keys: List[str] = []
        variants_table_headers_keys: List[str] = []
        mutations_table_headers_keys:  List[str] = []

        for results_set in graph_execution_results.values():
            stage_execution_results: ExecutionResults = results_set.get('stages')
            execution_results.update(stage_execution_results)

            experiment_metrics_sets = results_set.get('experiment_metrics_sets', {})

            for experiment_name, experiment in experiment_metrics_sets.items():
                experiment_summaries[experiment_name] = experiment.experiments_summary

                experiments_table_headers.update(
                    experiment.experiments_table_headers
                )

                variants_table_headers.update(
                    experiment.variants_table_headers
                )

                mutations_table_headers.update(
                    experiment.mutations_table_headers
                )

                if len(experiments_table_headers_keys) == 0:
                    experiments_table_headers_keys.extend(experiment.experiments_table_header_keys)

                if len(variants_table_headers_keys) == 0:
                    variants_table_headers_keys.extend(experiment.variants_table_header_keys)

                if len(mutations_table_headers_keys) == 0:
                    mutations_table_headers_keys.extend(experiment.mutations_table_headers_keys)

        self.execution_summary_table = ExecutionSummaryTable(execution_results)
        

        if len(experiment_summaries) > 0:
            self.experiment_summary_table = ExperimentsSummaryTable(
                experiment_summaries,
                experiments_table_headers=experiments_table_headers,
                variants_table_headers=variants_table_headers,
                experiments_table_headers_keys=experiments_table_headers_keys,
                variants_table_headers_keys=variants_table_headers_keys,
                mutations_table_headers=mutations_table_headers,
                mutations_table_headers_keys=mutations_table_headers_keys
            )

    def generate_tables(self):
        if self.experiment_summary_table:
            self.experiment_summary_table.generate_tables()

    def show_tables(self):
        if self.experiment_summary_table:
            self.experiment_summary_table.show_tables()