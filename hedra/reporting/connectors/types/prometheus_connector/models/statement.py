from .types import (
    Metric,
    PrometheusQuery,
    PrometheusMetric
)


class Statement:

    def __init__(self, query):
        self.metric = Metric(query)

    async def is_update(self):
        return self.metric.type in PrometheusMetric.types

    async def as_prometheus_query(self) -> PrometheusQuery:
        return PrometheusQuery(self.metric)

    async def as_prometheus_metric(self, registry=None) -> PrometheusMetric:
        return PrometheusMetric(self.metric, registry=registry)
