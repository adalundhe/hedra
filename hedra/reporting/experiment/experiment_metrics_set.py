import statistics
import uuid
from typing import Union, List, Dict, Any
from hedra.core.experiments.distribution_types import DistributionTypes
from hedra.reporting.metric import MetricsSet
from .experiments_collection import ExperimentMetricsCollection
from .experiment_metrics_set_types import (
    MutationSummary,
    VariantSummary,
    ExperimentSummary
)


RawSummaryItem = Dict[str, Union[str, int, float, bool, List[float]]]


class ExperimentMetricsSet:

    experiments_table_header_keys = [
        'experiment_name',
        'experiment_randomized',
        'experiment_completed',
        'experiment_succeeded',
        'experiment_failed',
        'experiment_median_aps'
    ]

    variants_table_header_keys = [
        'variant_name',
        'variant_experiment',
        'variant_weight',
        'variant_distribution',
        'variant_distribution_interval',
        'variant_ratio_completed',
        'variant_ratio_succeeded',
        'variant_ratio_failed',
        'variant_ratio_aps',
    ]

    variants_stats_table_header_keys = [
        'variant_name',
        'variant_completed',
        'variant_succeeded',
        'variant_failed',
        'variant_actions_per_second',
    ]
    
    mutations_table_headers_keys = [
        'mutation_name',
        'mutation_experiment_name',
        'mutation_variant_name',
        'mutation_chance',
        'mutation_targets',
        'mutation_type'
    ]

    def __init__(self) -> None:
        self.experiment_metrics_set_id = uuid.uuid4()
        self.experiment_name: Union[str, None] = None
        self.randomized: Union[bool, None] = None
        self.participants: List[str] = []
        self.variants: Dict[str, Dict[str, Any]] = {}
        self.mutations: Dict[str, List[Dict[str, Any]]] = {}
        self.metrics: Dict[str, Dict[str, MetricsSet]] = {}
        
        self.experiments_table_headers: Dict[str, str] = {}
        self.variants_table_headers: Dict[str, str] = {}
        self.mutations_table_headers: Dict[str, str] = {}
        self.mutations_summaries: Dict[str, str] = {}

        self.experiments_summary: ExperimentSummary = {}

    @classmethod
    def experiments_fields(cls):
        return cls.experiments_table_header_keys
    
    @classmethod
    def variants_fields(cls):
        return list(set([
            *cls.variants_table_header_keys,
            *cls.variants_stats_table_header_keys
        ]))
    
    @classmethod
    def mutations_fields(cls):
        return cls.mutations_table_headers_keys

    def generate_variant_summaries(self) -> Dict[str, VariantSummary]:

        variants_summaries = {}

        for participant in self.participants:
            variant = self.variants.get(participant)
            mutations = self.mutations.get(participant)
            metrics_set = self.metrics.get(participant)

            group_variant_summaries: Dict[str, Union[int, List[float]]] = {
                'variant_completed': 0,
                'variant_succeeded': 0,
                'variant_failed': 0,
                'variant_actions_per_second': []
            }
            for variant_metric_set in metrics_set.values():
                group_variant_summaries['variant_completed'] += variant_metric_set.common_stats.get('total')
                group_variant_summaries['variant_succeeded'] += variant_metric_set.common_stats.get('succeeded')
                group_variant_summaries['variant_failed'] += variant_metric_set.common_stats.get('failed')
                group_variant_summaries['variant_actions_per_second'].append(
                    variant_metric_set.common_stats.get('actions_per_second')
                )

            group_variant_summaries['variant_actions_per_second'] = round(
                statistics.median(
                    group_variant_summaries.get('variant_actions_per_second')
                ), 2
            )

            variant_distribution_type: DistributionTypes = variant.get('variant_distribution_type')

            variant_summary = VariantSummary(**{
                'variant_name': participant,
                'variant_weight': variant.get('variant_weight'),
                'variant_experiment': '',
                'variant_distribution': variant_distribution_type.name.capitalize(),
                'variant_distribution_interval': variant.get('variant_distribution_interval_duration'),
                'variant_mutation_summaries': {},
                **group_variant_summaries,
            })


            if len(mutations) > 0:

                for mutation in mutations:

                    mutation_name = mutation.get('mutation_name')

                    mutations_summary = MutationSummary(**{
                        'mutation_experiment_name': self.experiment_name,
                        'mutation_variant_name': participant,
                        'mutation_name': mutation_name,
                        'mutation_chance': mutation.get('mutation_chance'),
                        'mutation_targets': ':'.join(mutation.get('mutation_targets')),
                        'mutation_type': mutation.get('mutation_type')
                    })

                    self.mutations_table_headers.update({
                        header_name: header_name.replace(
                            'mutation_', ''
                        ).replace(
                            '_', ' '
                        ) for header_name in mutations_summary.dict().keys()
                    })
                        
                    self.mutations_summaries[mutation_name] = mutations_summary

                    variant_summary.variant_mutation_summaries[mutation_name] = mutations_summary

            variants_summaries[participant] = variant_summary

        return variants_summaries
    
    def generate_experiment_summary(self) -> None:
        variant_summaries = self.generate_variant_summaries()

        experiment_summary = ExperimentSummary(**{
            'experiment_name': self.experiment_name,
            'experiment_randomized': self.randomized,
            'experiment_completed': 0,
            'experiment_succeeded': 0,
            'experiment_failed': 0,
            'experiment_median_aps': 0.,
            'experiment_variant_summaries': {}
        })

        experiment_actions_per_second = []

        for participant in self.participants:

            selected_variant_summary = variant_summaries.get(participant)
            selected_variant_summary.variant_experiment = self.experiment_name

            variant_stats: Dict[str, List[Union[int, float]]] = {
                'alt_variants_completed': [],
                'alt_variants_succeeded': [],
                'alt_variants_failed': [],
                'alt_variants_actions_per_second': [],
            }

            for variant_name, variant_summary in variant_summaries.items():

                if variant_name != participant:

                    variant_stats['alt_variants_completed'].append(
                        variant_summary.variant_completed
                    )

                    variant_stats['alt_variants_succeeded'].append(
                        variant_summary.variant_succeeded
                    )

                    variant_stats['alt_variants_failed'].append(
                        variant_summary.variant_failed
                    )

                    variant_stats['alt_variants_actions_per_second'].append(
                        variant_summary.variant_actions_per_second
                    )
            
            variant_total_completed = selected_variant_summary.variant_completed
            mean_alt_variants_total_completed = statistics.mean(
                variant_stats.get('alt_variants_completed')
            )

            selected_variant_summary.variant_ratio_completed= round(
                variant_total_completed/mean_alt_variants_total_completed,
                2
            )
            
            variant_total_succeeded = selected_variant_summary.variant_succeeded
            mean_alt_variants_total_succeeded = statistics.mean(
                variant_stats.get('alt_variants_succeeded')
            )

            selected_variant_summary.variant_ratio_succeeded = round(
                variant_total_succeeded/mean_alt_variants_total_succeeded,
                2
            )

            variant_total_failed = selected_variant_summary.variant_failed
            mean_alt_variants_total_failed = statistics.mean(
                variant_stats.get('alt_variants_failed')
            )

            selected_variant_summary.variant_ratio_failed = round(
                variant_total_failed/mean_alt_variants_total_failed,
                2
            )

            variant_total_actions_per_second = selected_variant_summary.variant_actions_per_second
            median_alt_variants_actions_per_second = statistics.median(
                variant_stats.get('alt_variants_actions_per_second')
            )

            selected_variant_summary.variant_ratio_aps = round(
                variant_total_actions_per_second/median_alt_variants_actions_per_second,
                2
            )

            self.variants_table_headers.update({
                header_name: header_name.replace(
                    'variant_', ''
                ).replace(
                    '_', ' '
                ) for header_name in selected_variant_summary.dict().keys() if 'mutation' not in header_name
            })

            experiment_summary.experiment_variant_summaries[participant] = selected_variant_summary

            experiment_summary.experiment_completed += selected_variant_summary.variant_completed
            experiment_summary.experiment_succeeded += selected_variant_summary.variant_succeeded
            experiment_summary.experiment_failed += selected_variant_summary.variant_failed
            
            experiment_actions_per_second.append(
                selected_variant_summary.variant_actions_per_second
            )

        experiment_summary.experiment_median_aps = statistics.median(experiment_actions_per_second)

        self.experiments_table_headers = {
            header_name: header_name.replace(
                'experiment_', ''
            ).replace(
                '_', ' '
            ) for header_name in experiment_summary.dict().keys() if 'variant' not in header_name
        }
        
        self.experiments_summary = experiment_summary

    
    def split_experiments_metrics(self) -> ExperimentMetricsCollection:

        variant_records: List[Dict[str, str]] = []
        mutations_records: List[Dict[str, str]] = []
        mutations_summaries: List[MutationSummary] = []

        for variant in self.experiments_summary.experiment_variant_summaries.values():
            variant_records.append(
                variant.dict(
                    include={header for header in self.variants_fields()}
                )
            )

            for mutation in variant.variant_mutation_summaries.values():
                mutations_records.append(
                    mutation.dict(
                        include={header for header in self.mutations_table_headers_keys}
                    )
                )

                mutations_summaries.append(mutation)

        return ExperimentMetricsCollection(
            experiment=self.experiments_summary.dict(
                include={header for header in self.experiments_table_header_keys}
            ),
            variants=variant_records,
            mutations=mutations_records,
            experiment_summary=self.experiments_summary,
            variant_summaries=list(self.experiments_summary.experiment_variant_summaries.values()),
            mutation_summaries=mutations_summaries
        )
