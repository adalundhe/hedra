from typing import Dict, Union
from hedra.reporting.tags import Tag


class Metric:

    def __init__(
        self,
        name: str,
        source: str,
        stage: str, 
        data: Dict[str, Union[int, float, str, bool]], 
        tags: Dict[str, str]
    ) -> None:
        self.name = name
        self.stage = stage
        self.data = []
        self.fields = []
        self.values = []
        self.source = source
        self._raw_data = data
        self.tags = [
            Tag(tag_name, tag_value) for tag_name, tag_value in tags.items()
        ]

        for metric_name, metric_value in data.items():
            self.data.append((
                metric_name,
                metric_value
            ))

            self.fields.append(metric_name)
            self.values.append(metric_value)

        self.data = data

    @property
    def record(self):
        return {
            'name': self.name,
            'stage': self.stage,
            **self._raw_data
        }

    @property
    def stats(self):
        stats_metrics = {}
        for metric_name, metric_value in self._raw_data.items():
            if isinstance(metric_value, (int, float)):
                stats_metrics[metric_name] = metric_value

        return stats_metrics

    @property
    def quantiles(self):
        return self._raw_data.get('quantiles', [])