from enum import Enum


class MetricType(Enum):
    COUNT='COUNT'
    RATE='RATE'
    DISTRIBUTION='DISTRIBUTION'
    SAMPLE='SAMPLE'



metric_type_map = {
    'count': MetricType.COUNT,
    'rate': MetricType.RATE,
    'distribution': MetricType.DISTRIBUTION,
    'sample': MetricType.SAMPLE
}