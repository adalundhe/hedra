from collections import (
    OrderedDict,
    defaultdict
)
from tabulate import tabulate
from typing import Dict, List, Union
from hedra.logging import HedraLogger
from hedra.reporting.experiment.experiment_metrics_set_types import (
    ExperimentSummary
)



class ExperimentsSummaryTable:

    def __init__(
        self, 
        experiments_summaries: Dict[str, ExperimentSummary],
        experiment_headers: Dict[str, Dict[str, str]],
        experiment_headers_keys: Dict[str, List[str]]

    ) -> None:
        self.experiments_summaries = experiments_summaries
        self.experiment_headers = experiment_headers
        self.experiment_header_keys = experiment_headers_keys

        self.experiments_table: Union[str, None] = None
        self.variants_tables: Dict[str, Union[str, None]] = None
        self.variants_table_rows: Dict[str, List[OrderedDict]] = defaultdict(list)
        self.mutations_table: Union[str, None] = None
        self.logger = HedraLogger()
        self.logger.initialize()

        self.enabled_tables = {
            'experiments': False,
            'variants': False,
            'mutations': False
        }


    def generate_tables(self):
        self.experiments_table = self._generate_experiments_table()
        self.variants_tables = self._generate_variants_tables()
        self.mutations_table = self._generate_mutations_table()

    def show_tables(self):
        
        if any(self.enabled_tables.values()):
            self.logger.console.sync.info('\n-- Experiments --')

        if self.enabled_tables.get('experiments'):
            self.logger.console.sync.info('\nExperiments:\n')
            self.logger.console.sync.info(f'''{self.experiments_table}\n''')

        if self.enabled_tables.get('variants'):
            self.logger.console.sync.info('\nVariants:\n')

            variants_table = self.variants_tables.get('variants_table')
            self.logger.console.sync.info(f'''{variants_table}\n''')

        if self.mutations_table and self.enabled_tables.get('mutations'):
            self.logger.console.sync.info('\nMutations:\n')
            self.logger.console.sync.info(f'''{self.mutations_table}\n''')

    def _generate_experiments_table(self) -> str:

        experiment_table_rows: List[str] = []  

        header_keys = self.experiment_header_keys.get('experiments_table_headers_keys')
        
        for experiment_summary in self.experiments_summaries.values():

            table_row = OrderedDict()

            for field_name in header_keys:

                experiment_summary_dict = experiment_summary.dict()

                headers = self.experiment_headers.get('experiment_table_headers')
                header_name = headers.get(field_name)
                table_row[header_name] = experiment_summary_dict.get(field_name) 

            experiment_table_rows.append(table_row)


        return tabulate(
            list(sorted(
                experiment_table_rows,
                key=lambda row: row['name']
            )),
            headers='keys',
            missingval='None',
            tablefmt="simple",
            floatfmt='.2f'
        )

    def _generate_variants_tables(self) -> Dict[str, Union[str, None]]:
        
        variants_table_header_keys = self.experiment_header_keys.get('variant_table_headers_keys')
        variants_stats_table_header_keys = self.experiment_header_keys.get('variants_stats_table_header_keys')

        for experiment_summary in self.experiments_summaries.values():

            for variant_summary in experiment_summary.experiment_variant_summaries.values():
                table_row = OrderedDict()

                for field_name in variants_table_header_keys:

                    variant_summary_dict = variant_summary.dict()

                    headers = self.experiment_headers.get('variants_table_headers')
                    header_name = headers.get(field_name)
                    table_row[header_name] = variant_summary_dict.get(field_name) 

                self.variants_table_rows['variants_table_rows'].append(table_row)

                table_row = OrderedDict()

                for field_name in variants_stats_table_header_keys:

                    variant_summary_dict = variant_summary.dict()

                    headers = self.experiment_headers.get('variants_table_headers')
                    header_name = headers.get(field_name)
                    table_row[header_name] = variant_summary_dict.get(field_name) 

                self.variants_table_rows['variant_stats_table_rows'].append(table_row)

        variant_stats_table_rows = list(sorted(
            self.variants_table_rows.get('variant_stats_table_rows'),
            key=lambda row: row['name']
        ))

        for row in variant_stats_table_rows:
            del row['name']

        return {
            'variants_table': tabulate(
                list(sorted(
                    self.variants_table_rows.get('variants_table_rows'),
                    key=lambda row: row['name']
                )),
                headers='keys',
                missingval='None',
                tablefmt="simple",
                floatfmt=".2f"
            ),
            'variant_stats_table': tabulate(
                variant_stats_table_rows,
                headers='keys',
                missingval='None',
                tablefmt="simple",
                floatfmt=".2f"
            )
        }
    
    def _generate_mutations_table(self) -> str:

        mutation_table_rows: List[OrderedDict] = []

        mutations_table_headers_keys = self.experiment_header_keys.get('mutations_table_header_keys')

        for experiment_summary in self.experiments_summaries.values():

            for variant_summary in experiment_summary.experiment_variant_summaries.values():

                for mutation_summary in variant_summary.variant_mutation_summaries.values():

                    mutation_summary_dict = mutation_summary.dict()

                    table_row = OrderedDict()

                    for field_name in mutations_table_headers_keys:

                        headers = self.experiment_headers.get('mutations_table_headers')
                        header_name = headers.get(field_name)
                    
                        table_row[header_name] = mutation_summary_dict.get(field_name) 

                    mutation_table_rows.append(table_row)

        return tabulate(
            list(sorted(
                mutation_table_rows,
                key=lambda row: row['name']
            )),
            headers='keys',
            missingval='None',
            tablefmt="simple"
        )
                