from enum import Enum
from hedra.monitoring import (
    MemoryMonitor,
    CPUMonitor
)
from hedra.reporting.metric.metric_types import MetricType
from hedra.reporting.tags import Tag
from pydantic import (
    BaseModel,
    StrictStr,
    StrictInt,
    StrictFloat
)
from typing import (
    Dict,
    List,
    Union
)


MemoryMonitorGroup = Dict[str, MemoryMonitor]

CPUMonitorGroup = Dict[str, CPUMonitor]

MonitorGroup = Dict[str, Union[CPUMonitorGroup, MemoryMonitorGroup]]

StageSystemMetricsGroup = Dict[str, Dict[str, List[Union[int, float]]]]


class SystemMetricGroupType(Enum):
    CPU='cpu'
    MEMORY='memory'


class SessionMetricsCollection(BaseModel):
    name: StrictStr
    group: StrictStr
    mean: Union[StrictInt, StrictFloat]
    median: Union[StrictInt, StrictFloat]
    max: Union[StrictInt, StrictFloat]
    min: Union[StrictInt, StrictFloat]
    stdev: Union[StrictInt, StrictFloat]
    variance: Union[StrictInt, StrictFloat]
    quantile_10th: Union[StrictInt, StrictFloat]
    quantile_20th: Union[StrictInt, StrictFloat]
    quantile_25th: Union[StrictInt, StrictFloat]
    quantile_30th: Union[StrictInt, StrictFloat]
    quantile_40th: Union[StrictInt, StrictFloat]
    quantile_50th: Union[StrictInt, StrictFloat]
    quantile_60th: Union[StrictInt, StrictFloat]
    quantile_70th: Union[StrictInt, StrictFloat]
    quantile_75th: Union[StrictInt, StrictFloat]
    quantile_80th: Union[StrictInt, StrictFloat]
    quantile_90th: Union[StrictInt, StrictFloat]
    quantile_95th: Union[StrictInt, StrictFloat]
    quantile_99th: Union[StrictInt, StrictFloat]

    @property
    def stats(self) -> Dict[str, Union[int, float]]:
        return {
            'mean': self.mean,
            'median': self.median,
            'max': self.max,
            'min': self.min,
            'stdev': self.stdev,
            'variance': self.variance,
            'quantile_10th': self.quantile_10th,
            'quantile_20th': self.quantile_20th,
            'quantile_25th': self.quantile_25th,
            'quantile_30th': self.quantile_30th,
            'quantile_40th': self.quantile_40th,
            'quantile_50th': self.quantile_50th,
            'quantile_60th': self.quantile_60th,
            'quantile_70th': self.quantile_70th,
            'quantile_75th': self.quantile_75th,
            'quantile_80th': self.quantile_80th,
            'quantile_90th': self.quantile_90th,
            'quantile_95th': self.quantile_95th,
            'quantile_99th': self.quantile_99th
        }

    @property
    def record(self) -> Dict[str, Union[str, bool, int, float]]:
        return {
            'name': self.name,
            'group': self.group,
            **self.stats
        }
    
    @property
    def types_map(self):
        return {
            'mean': MetricType.SAMPLE,
            'median': MetricType.SAMPLE,
            'max': MetricType.SAMPLE,
            'min': MetricType.SAMPLE,
            'stdev': MetricType.SAMPLE,
            'variance': MetricType.SAMPLE,
            'quantile_10th': MetricType.SAMPLE,
            'quantile_20th': MetricType.SAMPLE,
            'quantile_25th': MetricType.SAMPLE,
            'quantile_30th': MetricType.SAMPLE,
            'quantile_40th': MetricType.SAMPLE,
            'quantile_50th': MetricType.SAMPLE,
            'quantile_60th': MetricType.SAMPLE,
            'quantile_70th': MetricType.SAMPLE,
            'quantile_75th': MetricType.SAMPLE,
            'quantile_80th': MetricType.SAMPLE,
            'quantile_90th': MetricType.SAMPLE,
            'quantile_95th': MetricType.SAMPLE,
            'quantile_99th': MetricType.SAMPLE,
        }
    
    @property
    def tags(self):
        tag_fields = {
            'name': self.name,
            'group': self.group
        }

        return [
            Tag(
                tag_field_name,
                tag_field_value
            ) for tag_field_name, tag_field_value in tag_fields.items()
        ]


class SystemMetricsCollection(BaseModel):
    stage: StrictStr
    name: StrictStr
    group: StrictStr
    mean: Union[StrictInt, StrictFloat]
    median: Union[StrictInt, StrictFloat]
    max: Union[StrictInt, StrictFloat]
    min: Union[StrictInt, StrictFloat]
    stdev: Union[StrictInt, StrictFloat]
    variance: Union[StrictInt, StrictFloat]
    quantile_10th: Union[StrictInt, StrictFloat]
    quantile_20th: Union[StrictInt, StrictFloat]
    quantile_25th: Union[StrictInt, StrictFloat]
    quantile_30th: Union[StrictInt, StrictFloat]
    quantile_40th: Union[StrictInt, StrictFloat]
    quantile_50th: Union[StrictInt, StrictFloat]
    quantile_60th: Union[StrictInt, StrictFloat]
    quantile_70th: Union[StrictInt, StrictFloat]
    quantile_75th: Union[StrictInt, StrictFloat]
    quantile_80th: Union[StrictInt, StrictFloat]
    quantile_90th: Union[StrictInt, StrictFloat]
    quantile_95th: Union[StrictInt, StrictFloat]
    quantile_99th: Union[StrictInt, StrictFloat]

    @property
    def stats(self) -> Dict[str, Union[int, float]]:
        return {
            'mean': self.mean,
            'median': self.median,
            'max': self.max,
            'min': self.min,
            'stdev': self.stdev,
            'variance': self.variance,
            'quantile_10th': self.quantile_10th,
            'quantile_20th': self.quantile_20th,
            'quantile_25th': self.quantile_25th,
            'quantile_30th': self.quantile_30th,
            'quantile_40th': self.quantile_40th,
            'quantile_50th': self.quantile_50th,
            'quantile_60th': self.quantile_60th,
            'quantile_70th': self.quantile_70th,
            'quantile_75th': self.quantile_75th,
            'quantile_80th': self.quantile_80th,
            'quantile_90th': self.quantile_90th,
            'quantile_95th': self.quantile_95th,
            'quantile_99th': self.quantile_99th
        }

    @property
    def record(self) -> Dict[str, Union[str, bool, int, float]]:
        return {
            'stage': self.stage,
            'name': self.name,
            'group': self.group,
            **self.stats
        }
    
    @property
    def types_map(self):
        return {
            'mean': MetricType.SAMPLE,
            'median': MetricType.SAMPLE,
            'max': MetricType.SAMPLE,
            'min': MetricType.SAMPLE,
            'stdev': MetricType.SAMPLE,
            'variance': MetricType.SAMPLE,
            'quantile_10th': MetricType.SAMPLE,
            'quantile_20th': MetricType.SAMPLE,
            'quantile_25th': MetricType.SAMPLE,
            'quantile_30th': MetricType.SAMPLE,
            'quantile_40th': MetricType.SAMPLE,
            'quantile_50th': MetricType.SAMPLE,
            'quantile_60th': MetricType.SAMPLE,
            'quantile_70th': MetricType.SAMPLE,
            'quantile_75th': MetricType.SAMPLE,
            'quantile_80th': MetricType.SAMPLE,
            'quantile_90th': MetricType.SAMPLE,
            'quantile_95th': MetricType.SAMPLE,
            'quantile_99th': MetricType.SAMPLE,
        }
    
    @property
    def tags(self):
        tag_fields = {
            'stage': self.stage,
            'name': self.name,
            'group': self.group
        }

        return [
            Tag(
                tag_field_name,
                tag_field_value
            ) for tag_field_name, tag_field_value in tag_fields.items()
        ]