import uuid
from typing import Any, Dict, List, Union
from hedra.reporting.tags import Tag
from .metrics_group import MetricsGroup


class MetricsSet:

    def __init__(
        self,
        name: str,
        source: str,
        stage: str, 
        metrics_data: Dict[str, Dict[str, Any]],
        tags: Dict[str, str]
    ) -> None:

        self.metrics_set_id = str(uuid.uuid4())
        self.groups: Dict[str, MetricsGroup] = {}

        self.name = name
        self.source = source
        self.stage = stage
        self._raw_metrics = metrics_data
        self._raw_tags = tags

        self.fields = [
            'name',
            'stage'
        ]
        self.values = [
            name,
            stage
        ]

        self.common_stats = {
            'total': metrics_data.get('total'),
            'succeeded': metrics_data.get('succeeded'),
            'failed': metrics_data.get('failed'),
            'actions_per_second': metrics_data.get('actions_per_second')
        }

        self.custom_metrics = metrics_data.get('custom')
        self.errors: List[Dict[str, Union[str, int]]] = metrics_data.get('errors')
        self.tags = [
            Tag(tag_name, tag_value) for tag_name, tag_value in tags.items()
        ]

        metrics_groups = metrics_data.get('groups', {})
        for group_name, group in metrics_groups.items():
            self.groups[group_name] = MetricsGroup(
                name,
                source,
                stage,
                group_name,
                group,
                self.common_stats
            )

        record_fields = self.groups.get('total').fields
        custom_fields = list(self.groups.get(
            'total'
        ).custom_fields.keys())

        self.fields.extend(record_fields)
        self.fields.extend(custom_fields)

        self.quantiles = list(self.groups.get(
            'total'
        ).quantiles.keys())

        self.custom_field_names = self.custom_fields = self.groups.get(
            'total'
        ).custom_field_names


        self.custom_fields = self.groups.get(
            'total'
        ).custom_fields

        self.custom_schemas = self.groups.get(
            'total'
        ).custom_schemas

        self.stats_fields = list(self.groups.get(
            'total'
        ).stats.keys())

        

    def serialize(self):
        return {
            'name': self.name,
            'source': self.source,
            'stage': self.stage,
            'metrics_data': self._raw_metrics,
            'tags': self._raw_tags
        }