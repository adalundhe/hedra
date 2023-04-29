from typing import Dict, List
from .metrics_set_types import GroupMetricsSet
from .metrics_set import MetricsSet


class StageMetricsSummary:

    def __init__(self) -> None:
        self.total_completed: int = 0
        self.actions_per_second: float = 0.
        self.total_time: float = 0.
        self.metrics_sets: Dict[str, MetricsSet] = {}
        self.action_and_task_metrics: Dict[str, GroupMetricsSet] = {}

        self.stage_table_header_keys = [
            'stage_name',
            'persona',
            'batch_size',
            'completed',
            'succeeded',
            'failed',
            'actions_per_second',
            'total_time',
            'waiting',
            'connecting',
            'reading',
            'writing'
        ]

        self.stage_table_headers = {
            header_name: header_name.replace(
                '_', ' '
            ) for header_name in self.stage_table_header_keys
        }

        self.groups: List[str] = [
            'total',
            'waiting',
            'connecting',
            'writing',
            'reading'
        ]

        self.fields: List[str] = [
            'median',
            'mean',
            'variance',
            'stdev',
            'minimum',
            'maximum',
            'quantile_10th',
            'quantile_20th',
            'quantile_30th',
            'quantile_40th',
            'quantile_50th',
            'quantile_60th',
            'quantile_70th',
            'quantile_80th',
            'quantile_90th',
            'quantile_95th',
            'quantile_99th'
        ]

        self.action_or_task_header_keys: List[str] = []

        for group in self.groups:
            for field in self.fields:
                self.action_or_task_header_keys.append(
                    f'{group}_{field}'
                )

        self.action_or_task_headers = {
            header_name: header_name.replace(
                '_', ' '
            ) for header_name in self.action_or_task_header_keys
        }

    def calculate_action_and_task_metrics(self):

        for action_or_task_name, metric_set in self.metrics_sets.items():

            grouped_metrics = {}

            for group_name, group_metrics in metric_set.groups.items():
                for field_name, field_value in group_metrics.record.items():
                    if isinstance(field_value, (int, float,)):
                        metric_key = f'{group_name}_{field_name}'
                        grouped_metrics[metric_key] = field_value


            self.action_and_task_metrics[action_or_task_name] = GroupMetricsSet(**grouped_metrics)
