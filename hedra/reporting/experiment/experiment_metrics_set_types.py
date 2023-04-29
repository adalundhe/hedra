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


class VariantSummary(BaseModel):
    variant_name: StrictStr
    variant_experiment: StrictStr
    variant_weight: StrictFloat
    variant_distribution: StrictStr
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


class ExperimentSummary(BaseModel):
    experiment_name: StrictStr
    experiment_randomized: StrictBool
    experiment_completed: StrictInt
    experiment_succeeded: StrictInt
    experiment_failed: StrictInt
    experiment_median_aps: StrictFloat
    experiment_variant_summaries: Dict[str, VariantSummary]