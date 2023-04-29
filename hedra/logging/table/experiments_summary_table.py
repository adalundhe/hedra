from collections import OrderedDict
from tabulate import tabulate
from typing import Dict, List, Union
from hedra.logging import HedraLogger


MutationSummary = Dict[str, Union[int, float, bool, str]]

VariantSummary = Dict[str, Union[int, float, bool, str, Dict[str, MutationSummary]]]

ExperimentSummary = Dict[str, Union[int, float, Dict[str, VariantSummary], List[str]]]


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
            self.logger.console.sync.info(f'''{self.mutations_table}\n''')

    def _generate_experiments_table(self) -> str:

        experiment_table_rows: List[str] = []  
        
        for experiment_summary in self.experiments_summaries.values():

            table_row = OrderedDict()

            for field_name in self.experiments_table_headers_keys:
                header_name = self.experiments_table_headers.get(field_name)
                table_row[header_name] = experiment_summary.get(field_name) 

            experiment_table_rows.append(table_row)


        return tabulate(
            experiment_table_rows,
            headers='keys',
            missingval='None',
            tablefmt="simple_outline"
        )

    def _generate_variants_table(self) -> str:

        variant_table_rows: List[str] = []

        for experiment_summary in self.experiments_summaries.values():
            experiment_variants = experiment_summary.get('experiment_variant_summaries')

            for variant_summary in experiment_variants.values():
                table_row = OrderedDict()

                for field_name in self.variants_table_headers_keys:
                    header_name = self.variants_table_headers.get(field_name)
                    table_row[header_name] = variant_summary.get(field_name) 

                variant_table_rows.append(table_row)

        return tabulate(
            variant_table_rows,
            headers='keys',
            missingval='None',
            tablefmt="simple_outline"
        )
    
    def _generate_mutations_table(self) -> str:

        mutation_table_rows: List[str] = []

        for experiment_summary in self.experiments_summaries.values():
            experiment_variants = experiment_summary.get('experiment_variant_summaries')

            for variant_summary in experiment_variants.values():

                variant_mutations = variant_summary.get('variant_mutation_summaries')

                for mutation_summary in variant_mutations.values():
                    table_row = OrderedDict()

                    for field_name in self.mutations_table_headers_keys:
                        header_name = self.mutations_table_headers.get(field_name)
                        table_row[header_name] = mutation_summary.get(field_name) 

                    mutation_table_rows.append(table_row)

        return tabulate(
            mutation_table_rows,
            headers='keys',
            missingval='None',
            tablefmt="simple_outline"
        )
                