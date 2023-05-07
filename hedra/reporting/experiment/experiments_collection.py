from pydantic import (
    BaseModel, 
    StrictBool, 
    StrictFloat, 
    StrictInt, 
    StrictStr
)
from typing import List, Dict, Union
from .experiment_metrics_set_types import (
    ExperimentSummary,
    VariantSummary,
    MutationSummary
)


class ExperimentMetricsCollection(BaseModel):
    experiment: Dict[str, Union[StrictBool, StrictFloat, StrictInt, StrictStr]]
    variants: List[Dict[str, Union[StrictBool, StrictFloat, StrictInt, StrictStr]]]
    mutations: List[Dict[str, Union[StrictBool, StrictFloat, StrictInt, StrictStr]]]

    experiment_summary: ExperimentSummary
    variant_summaries: List[VariantSummary]
    mutation_summaries: List[MutationSummary]


class ExperimentMetricsCollectionSet(BaseModel):
    experiments_metrics_fields: List[str]
    variants_metrics_fields: List[str]
    mutations_metrics_fields: List[str]
    experiments: List[Dict[str, Union[StrictBool, StrictFloat, StrictInt, StrictStr]]]
    variants: List[Dict[str, Union[StrictBool, StrictFloat, StrictInt, StrictStr]]]
    mutations: List[Dict[str, Union[StrictBool, StrictFloat, StrictInt, StrictStr]]]

    experiment_summaries: List[ExperimentSummary]
    variant_summaries: List[VariantSummary]
    mutation_summaries: List[MutationSummary]
    