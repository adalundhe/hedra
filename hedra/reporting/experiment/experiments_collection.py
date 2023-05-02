from pydantic import BaseModel, StrictBool, StrictFloat, StrictInt, StrictStr
from typing import List, Dict, Union


class ExperimentMetricsCollection(BaseModel):
    experiment: Dict[str, Union[StrictBool, StrictFloat, StrictInt, StrictStr]]
    variants: List[Dict[str, Union[StrictBool, StrictFloat, StrictInt, StrictStr]]]
    mutations: List[Dict[str, Union[StrictBool, StrictFloat, StrictInt, StrictStr]]]


class ExperimentMetricsCollectionSet(BaseModel):
    experiments_metrics_fields: List[str]
    variants_metrics_fields: List[str]
    mutations_metrics_fields: List[str]
    experiments: List[Dict[str, Union[StrictBool, StrictFloat, StrictInt, StrictStr]]]
    variants: List[Dict[str, Union[StrictBool, StrictFloat, StrictInt, StrictStr]]]
    mutations: List[Dict[str, Union[StrictBool, StrictFloat, StrictInt, StrictStr]]]
    