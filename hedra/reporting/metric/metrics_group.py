from typing import Any, Dict, List, Union
from hedra.reporting.tags import Tag
from .timings_group import TimingsGroup


class MetricsGroup:

    def __init__(
        self,
        name: str,
        source: str,
        stage: str, 
        data: Dict[str, Dict[str, Any]], 
        tags: Dict[str, str]
    ) -> None:
        self.groups: Dict[str, TimingsGroup] = {}

        self.name = name
        self.stage = stage
        self.fields = [
            'name',
            'stage'
        ]
        self.values = [
            name,
            stage
        ]

        self.common_stats = {
            'total': data.get('total'),
            'succeeded': data.get('succeeded'),
            'failed': data.get('failed')
        }

        self.errors: List[Dict[str, Union[str, int]]] = data.get('errors')
        self.tags = [
            Tag(tag_name, tag_value) for tag_name, tag_value in tags.items()
        ]

        timings = data.get('timings', {})
        for timings_group_name, timings_group in timings.items():
            self.groups[timings_group_name] = TimingsGroup(
                name,
                source,
                stage,
                timings_group_name,
                timings_group,
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

        

