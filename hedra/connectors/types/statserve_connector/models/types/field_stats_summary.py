from __future__ import annotations
from .metric import Metric

class FieldStatsSummary:

    def __init__(self, metrics_summary):
        self._metrics_summary = metrics_summary
        self.stats = metrics_summary.get('stats', {})
        self.counts = metrics_summary.get('counts', {})
        self.quantiles = metrics_summary.get('quantiles', {})
        self.metadata = metrics_summary.get('metadata')
        self.summary = []

        for stat in self.stats:
            self.summary.append(
                Metric({
                    **self.metadata,
                    'metric_stat': stat,
                    'value': self.stats.get(stat)
                })
            )

        for count in self.counts:
            self.summary.append(
                Metric({
                    **self.metadata,
                    'metric_stat': count,
                    'value': self.counts.get(count)
                })
            )

        for quantile in self.quantiles:
            self.summary.append(
                Metric({
                    **self.metadata,
                    'metric_stat': quantile,
                    'value': self.quantiles.get(quantile)
                })
            )

    def to_dict_list(self) -> list:
        return [
            metric.to_dict() for metric in self.summary
        ]