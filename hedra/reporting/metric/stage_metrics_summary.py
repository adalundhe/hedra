import statistics
from decimal import Decimal
from typing import Dict, List, Optional, Union
from .metrics_set_types import (
    GroupMetricsSet,
    StageMetrics,
    CommonMetrics
)
from hedra.core.personas.streaming.stream_analytics import StreamAnalytics
from .stage_streams_set import StageStreamsSet
from .metrics_set import MetricsSet


class StageMetricsSummary:

    def __init__(
        self,
        stage_name: str=None,
        persona_type: str=None,
        batch_size: int=None,
        total_elapsed: float=None,
        stage_streamed_analytics: Optional[List[StreamAnalytics]]=None
    ) -> None:

        self.metrics_sets: Dict[str, MetricsSet] = {}
        self.action_and_task_metrics: Dict[str, GroupMetricsSet] = {}

        self.stage_streamed_analytics = stage_streamed_analytics

        initial_stream = []
        if self.stage_streamed_analytics:
            initial_stream = [
                0 for _ in range(len(
                    self.stage_streamed_analytics[0].interval_completed_counts
                ))
            ]

        persona_type_slug = '-'.join([
            segment.capitalize() for segment in persona_type.split('-')
        ])

        self.stage_metrics = StageMetrics(**{
            'name': stage_name,
            'persona': persona_type_slug,
            'batch_size': batch_size,
            'total': 0,
            'succeeded': 0,
            'failed': 0,
            'aps': 0.,
            'time': total_elapsed,
            'μ_sec': 0.,
            'μ_waiting': 0.,
            'μ_connecting': 0.,
            'μ_reading': 0.,
            'μ_writing': 0.,
            'streamed_completion_rates': list(initial_stream),
            'streamed_completed': list(initial_stream),
            'streamed_succeeded': list(initial_stream),
            'streamed_failed': list(initial_stream),
            'streamed_batch_timings': list(initial_stream),
        })

        self.stage_table_header_keys = [
            'name',
            'time',
            'persona',
            'batch_size',
            'total',
            'succeeded',
            'failed',
            'aps',
            'μ_sec',
            'μ_waiting',
            'μ_connecting',
            'μ_reading',
            'μ_writing'
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

        self.common_fields: List[str] = [
            'total',
            'succeeded',
            'failed'
        ]

        self.fields: List[str] = [
            'med',
            'μ',
            'var',
            'std',
            'min',
            'max',
            'q_10',
            'q_20',
            'q_30',
            'q_40',
            'q_50',
            'q_60',
            'q_70',
            'q_80',
            'q_90',
            'q_95',
            'q_99'
        ]

        self._record_fields: List[str] = [
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

        self._record_fields_map: Dict[str, str] = {}

        for record_field, table_field in zip(self._record_fields, self.fields):
            self._record_fields_map[record_field] = table_field
        
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

        self.common_metrics: Dict[str, CommonMetrics] = {}

    @property
    def streams(self):
        return StageStreamsSet(
            self.stage_metrics.name,
            self.stage_streamed_analytics
        )

    def calculate_action_and_task_metrics(self):
        self._group_action_and_task_metrics()
        self._calculate_stage_totals()

        if self.stage_streamed_analytics:
            self._calculate_completed_rate_stream()
            self._calculate_succeeded_rate_stream()
            self._calculate_failed_rate_stream()
            self._calculate_batch_timings_stream()
            self._calculate_completion_rate_stream()

    def _group_action_and_task_metrics(self):

        for action_or_task_name, metric_set in self.metrics_sets.items():

            self.stage_metrics.total += metric_set.common_stats.get('total')
            self.stage_metrics.succeeded += metric_set.common_stats.get('succeeded')
            self.stage_metrics.failed += metric_set.common_stats.get('failed')

            grouped_metrics = {}
            common_metrics = {}

            for group_name, group_metrics in metric_set.groups.items():

                for field_name, field_value in group_metrics.record.items():
                    if isinstance(field_value, (int, float,)):

                        table_field_name = self._record_fields_map.get(field_name)
                        metric_key = f'{group_name}_{table_field_name}'

                        grouped_metrics[metric_key] = field_value

            for table_key in self.common_fields:
                common_metrics[table_key] = metric_set.common_stats.get(table_key)
            
            common_metrics['aps'] = metric_set.common_stats.get('actions_per_second')

            self.common_metrics[action_or_task_name] = CommonMetrics(**common_metrics)
            self.action_and_task_metrics[action_or_task_name] = GroupMetricsSet(**grouped_metrics)
        
    def _calculate_stage_totals(self):
        self.stage_metrics.aps = round(
            self.stage_metrics.total/self.stage_metrics.time,
            2
        )

        for group_metrics_set in self.action_and_task_metrics.values():
            self.stage_metrics.μ_sec = group_metrics_set.total_μ
            self.stage_metrics.μ_waiting += group_metrics_set.waiting_μ
            self.stage_metrics.μ_connecting += group_metrics_set.connecting_μ
            self.stage_metrics.μ_writing += group_metrics_set.writing_μ
            self.stage_metrics.μ_reading += group_metrics_set.reading_μ


        self.stage_metrics.μ_sec = Decimal(
            self.stage_metrics.μ_sec/self.stage_metrics.total
        )

        self.stage_metrics.μ_waiting = Decimal(
            self.stage_metrics.μ_waiting/self.stage_metrics.total
        )

        self.stage_metrics.μ_connecting = Decimal(
            self.stage_metrics.μ_connecting/self.stage_metrics.total
        )

        self.stage_metrics.μ_reading = Decimal(
            self.stage_metrics.μ_reading/self.stage_metrics.total
        )

        self.stage_metrics.μ_writing = Decimal(
            self.stage_metrics.μ_writing/self.stage_metrics.total
        )

    def _calculate_completion_rate_stream(self):
        bins_count = max(
            len(stream.interval_completion_rates) for stream in self.stage_streamed_analytics
        )
        self.stage_metrics.streamed_completion_rates = [0 for _ in range(bins_count)]
        
        for idx in range(bins_count):

            self.stage_metrics.streamed_completion_rates[idx] = round(
                self.stage_metrics.streamed_completed[idx]/self.stage_metrics.streamed_batch_timings[idx],
                2
            )

    def _calculate_succeeded_rate_stream(self):
        bins_count = max(
            len(stream.interval_succeeded_counts) for stream in self.stage_streamed_analytics
        )

        self.stage_metrics.streamed_succeeded = [0 for _ in range(bins_count)]

        for stream in self.stage_streamed_analytics:
            for idx, succeeded_count in enumerate(stream.interval_succeeded_counts):
                self.stage_metrics.streamed_succeeded[idx] += succeeded_count        

        total_succeeded = sum(self.stage_metrics.streamed_succeeded) 

        if total_succeeded > self.stage_metrics.succeeded:
            stream_error = total_succeeded - self.stage_metrics.succeeded
            binned_error = int(stream_error/bins_count)
            remainder_error = stream_error%bins_count

            carried = 0
            for idx in range(bins_count):
                error = binned_error + carried
                corrected_amount = self.stage_metrics.streamed_succeeded[idx] - error

                if corrected_amount > 0:
                    self.stage_metrics.streamed_succeeded[idx] = corrected_amount
                    carried = 0

                else:
                    carried += error

            self.stage_metrics.streamed_succeeded[-1] -= remainder_error

    def _calculate_failed_rate_stream(self):
        bins_count = max(
            len(stream.interval_failed_counts) for stream in self.stage_streamed_analytics
        )

        self.stage_metrics.streamed_failed = [0 for _ in range(bins_count)]

        for stream in self.stage_streamed_analytics:
            for idx, failed_count in enumerate(stream.interval_failed_counts):
                self.stage_metrics.streamed_failed[idx] += failed_count

        total_failed = sum(self.stage_metrics.streamed_failed) 

        if total_failed > self.stage_metrics.failed:
            stream_error = total_failed - self.stage_metrics.failed
            binned_error = int(stream_error/bins_count)
            remainder_error = stream_error%bins_count

            carried = 0
            for idx in range(bins_count):
                error = binned_error + carried
                corrected_amount = self.stage_metrics.streamed_failed[idx] - error

                if corrected_amount > 0:
                    self.stage_metrics.streamed_failed[idx] = corrected_amount
                    carried = 0

                else:
                    carried += error

            self.stage_metrics.streamed_failed[-1] -= remainder_error

    def _calculate_completed_rate_stream(self):
        bins_count = max([
            len(stream.interval_completed_counts) for stream in self.stage_streamed_analytics
        ])
        self.stage_metrics.streamed_completed = [0 for _ in range(bins_count)]

        for stream in self.stage_streamed_analytics:
            for idx, completed_count in enumerate(stream.interval_completed_counts):
                self.stage_metrics.streamed_completed[idx] += completed_count

        total_completed = sum(self.stage_metrics.streamed_completed) 

        if total_completed > self.stage_metrics.total:
            stream_error = total_completed - self.stage_metrics.total
            binned_error = int(stream_error/bins_count)
            remainder_error = stream_error%bins_count

            carried = 0
            for idx in range(bins_count):
                error = binned_error + carried
                corrected_amount = self.stage_metrics.streamed_completed[idx] - error

                if corrected_amount > 0:
                    self.stage_metrics.streamed_completed[idx] = corrected_amount
                    carried = 0

                else:
                    carried += error

            self.stage_metrics.streamed_completed[-1] -= remainder_error

    def _calculate_batch_timings_stream(self):
        bins_count = max([
            len(stream.interval_batch_timings) for stream in self.stage_streamed_analytics
        ])

        streamed_batch_timings = [[] for _ in range(bins_count)]

        for stream in self.stage_streamed_analytics:
            for idx, batch_timing in enumerate(stream.interval_batch_timings):
                streamed_batch_timings[idx].append(batch_timing)

        for idx, batch_timings in enumerate(streamed_batch_timings):
            streamed_batch_timings[idx] = statistics.median(batch_timings)

        for idx, batch_timing in enumerate(streamed_batch_timings):
            streamed_batch_timings[idx] = round(
                batch_timing,
                2
            )

        self.stage_metrics.streamed_batch_timings = streamed_batch_timings
