from hedra.reporting.metric.metric_types import MetricType, metric_type_map
from pydantic import (
    BaseModel, 
    StrictStr, 
    StrictBool, 
    StrictInt, 
    StrictFloat
)
from typing import (
    Dict, 
    Union,
    List
)




class MutationSummary(BaseModel):
    mutation_experiment_name: StrictStr
    mutation_variant_name: StrictStr
    mutation_name: StrictStr
    mutation_chance: StrictFloat
    mutation_targets: StrictStr
    mutation_type: StrictStr

    def stats(self):
        return {
            'mutation_chance': self.mutation_chance
        }
    
    @property
    def types_map(self):
        return {
            'mutation_chance': MetricType.SAMPLE
        }


class VariantSummary(BaseModel):
    variant_name: StrictStr
    variant_experiment: StrictStr
    variant_weight: StrictFloat
    variant_distribution: Union[StrictStr, None]
    variant_distribution_interval: Union[StrictInt, StrictFloat]
    variant_completed: StrictInt
    variant_succeeded: StrictInt
    variant_failed: StrictInt
    variant_actions_per_second: Union[List[StrictFloat], StrictFloat]
    variant_mutation_summaries: Dict[str, MutationSummary]
    variant_ratio_completed: Union[StrictFloat, None]
    variant_ratio_succeeded: Union[StrictFloat, None]
    variant_ratio_failed: Union[StrictFloat, None]
    variant_ratio_aps: Union[StrictFloat, None]

    def stats(self):
        return {
            'variant_weight': self.variant_weight,
            'variant_distribution_interval': self.variant_distribution_interval,
            'variant_completed': self.variant_completed,
            'variant_succeeded': self.variant_succeeded,
            'variant_failed': self.variant_failed,
            'variant_actions_per_second': self.variant_actions_per_second,
            'variant_ratio_completed': self.variant_ratio_completed,
            'variant_ratio_succeeded': self.variant_ratio_succeeded,
            'variant_ratio_failed': self.variant_ratio_failed,
            'variant_ratio_aps': self.variant_ratio_aps
        }
    
    @property
    def types_map(self):
        return {
            'variant_weight': MetricType.SAMPLE,
            'variant_distribution_interval': MetricType.SAMPLE,
            'variant_completed': MetricType.COUNT,
            'variant_succeeded': MetricType.COUNT,
            'variant_failed': MetricType.COUNT,
            'variant_actions_per_second': MetricType.RATE,
            'variant_ratio_completed': MetricType.SAMPLE,
            'variant_ratio_succeeded': MetricType.SAMPLE,
            'variant_ratio_failed': MetricType.SAMPLE,
            'variant_ratio_aps': MetricType.SAMPLE
        }


class ExperimentSummary(BaseModel):
    experiment_name: StrictStr
    experiment_randomized: StrictBool
    experiment_completed: StrictInt
    experiment_succeeded: StrictInt
    experiment_failed: StrictInt
    experiment_median_aps: StrictFloat
    experiment_variant_summaries: Dict[str, VariantSummary]

    def stats(self):
        return {
            'experiment_completed': self.experiment_completed,
            'experiment_succeeded': self.experiment_succeeded,
            'experiment_failed': self.experiment_failed,
            'experiment_median_aps': self.experiment_median_aps
        }
    
    @property
    def types_map(self):
        return {
            'experiment_completed': MetricType.COUNT,
            'experiment_succeeded': MetricType.COUNT,
            'experiment_failed': MetricType.COUNT,
            'experiment_median_aps': MetricType.SAMPLE
        }