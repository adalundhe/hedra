from typing import Union
from .metric_types import (
    metric_type_map,
    MetricType
)


class CustomMetric:

    def __init__(
            self, 
            metric_name: str, 
            metric_shortname: str,
            metric_value: Union[float, int],
            metric_group: str=None,
            metric_type: str='sample'
        ) -> None:
        self.metric_shortname: str = metric_shortname
        self.metric_name: str = metric_name
        self.metric_value: Union[float, int] = metric_value
        self.metric_group = metric_group
        self.metric_type = metric_type_map.get(
            metric_type,
            MetricType.SAMPLE
        )