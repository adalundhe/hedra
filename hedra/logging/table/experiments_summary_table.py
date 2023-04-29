from collections import OrderedDict
from tabulate import tabulate
from typing import Dict, List, Union
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiment_metrics_set_types import (
    MutationSummary,
    VariantSummary,
    ExperimentSummary
)



class ExperimentsSummaryTable:

    def __init__(
        self, 
        experiments_summaries: Dict[str, ExperimentSummary]={},
        experiments_table_headers: Dict[str, str]={},
        variants_table_headers: Dict[str, str]={},
        mutations_table_headers: Dict[str, str]={},
        experiments_table_headers_keys: List[str]=[],
        variants_table_headers_keys: List[str]=[],
        mutations_table_headers_keys: List[str]=[]
    ) -> None:
        self.experiments_summaries = experiments_summaries

        self.experiments_table_headers = experiments_table_headers
        self.variants_table_headers = variants_table_headers
        self.mutations_table_headers = mutations_table_headers

        self.experiments_table_headers_keys = experiments_table_headers_keys
        self.variants_table_headers_keys = variants_table_headers_keys
        self.mutations_table_headers_keys = mutations_table_headers_keys

        self.experiments_table: Union[str, None] = None
        self.variants_table: Union[str, None] = None
        self.mutations_table: Union[str, None] = None
        self.logger = HedraLogger()
        self.logger.initialize()


    def generate_tables(self):
        self.experiments_table = self._generate_experiments_table()
        self.variants_table = self._generate_variants_table()
        self.mutations_table = self._generate_mutations_table()

    def show_tables(self):

        if self.experiments_table:
            self.logger.console.sync.info('\nExperiments:')
            self.logger.console.sync.info(f'''{self.experiments_table}\n''')


        if self.variants_table:
            self.logger.console.sync.info('Variants:')
            self.logger.console.sync.info(f'''{self.variants_table}\n''')

        if self.mutations_table:
            self.logger.console.sync.info('Mutations:')
            self.logger.console.sync.info(f'''{self.mutations_table}''')

    def _generate_experiments_table(self) -> str:

        experiment_table_rows: List[str] = []  
        
        for experiment_summary in self.experiments_summaries.values():

            table_row = OrderedDict()

            for field_name in self.experiments_table_headers_keys:

                experiment_summary_dict = experiment_summary.dict()

                header_name = self.experiments_table_headers.get(field_name)
                table_row[header_name] = experiment_summary_dict.get(field_name) 

            experiment_table_rows.append(table_row)


        return tabulate(
            list(sorted(
                experiment_table_rows,
                key=lambda row: row['name']
            )),
            headers='keys',
            missingval='None',
            tablefmt="simple_outline"
        )

    def _generate_variants_table(self) -> str:

        variant_table_rows: List[OrderedDict] = []

        for experiment_summary in self.experiments_summaries.values():

            for variant_summary in experiment_summary.experiment_variant_summaries.values():
                table_row = OrderedDict()

                for field_name in self.variants_table_headers_keys:

                    variant_summary_dict = variant_summary.dict()

                    header_name = self.variants_table_headers.get(field_name)
                    table_row[header_name] = variant_summary_dict.get(field_name) 

                variant_table_rows.append(table_row)

        return tabulate(
            list(sorted(
                variant_table_rows,
                key=lambda row: row['name']
            )),
            headers='keys',
            missingval='None',
            tablefmt="simple_outline"
        )
    
    def _generate_mutations_table(self) -> str:

        mutation_table_rows: List[OrderedDict] = []

        for experiment_summary in self.experiments_summaries.values():

            for variant_summary in experiment_summary.experiment_variant_summaries.values():

                for mutation_summary in variant_summary.variant_mutation_summaries.values():

                    mutation_summary_dict = mutation_summary.dict()

                    table_row = OrderedDict()

                    for field_name in self.mutations_table_headers_keys:
                        header_name = self.mutations_table_headers.get(field_name)
                        table_row[header_name] = mutation_summary_dict.get(field_name) 

                    mutation_table_rows.append(table_row)

        return tabulate(
            list(sorted(
                mutation_table_rows,
                key=lambda row: row['name']
            )),
            headers='keys',
            missingval='None',
            tablefmt="simple_outline"
        )
                