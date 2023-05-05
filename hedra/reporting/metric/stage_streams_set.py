import statistics
import numpy
import uuid
from hedra.reporting.metric.metric_types import MetricType
from hedra.core.personas.streaming.stream_analytics import StreamAnalytics
from typing import List, Dict, Union, Callable


CalculationMethod = Callable[[Dict[str,List[Union[int, float]]]], Dict[str, Union[int, float]]]


class StageStreamsSet:

    def __init__(self, 
        stage: str,
        stage_streams: List[StreamAnalytics]
    ) -> None:
        self.stage = stage
        self.stream_set_id = uuid.uuid4()
        self.interval_completion_rates: List[float] = []
        self.interval_completed_counts: List[int] = []
        self.interval_succeeded_counts: List[int] = []
        self.interval_failed_counts: List[int] = []
        self.interval_batch_timings: List[float] = []

        for stream in stage_streams:
            self.interval_completion_rates.extend(stream.interval_completion_rates)
            self.interval_completed_counts.extend(stream.interval_completed_counts)
            self.interval_succeeded_counts.extend(stream.interval_succeeded_counts)
            self.interval_failed_counts.extend(stream.interval_failed_counts)
            self.interval_batch_timings.extend(stream.interval_batch_timings)

        self._stat_types: Dict[str, CalculationMethod] = {
            'median': self._calculate_median,
            'mean': self._calculate_mean,
            'max': self._calculate_max,
            'min': self._calculate_min,
            'stdev': self._calculate_stdev,
            'variance': self._calculate_var,
        }

        self.quantiles = [
            10,
            20,
            25,
            30,
            40,
            50,
            60,
            70,
            75,
            80,
            90,
            95,
            99
        ]

        self.stats: Dict[str, List[Union[int, float]]] = {
            'completed': self.interval_completed_counts,
            'succeeded': self.interval_succeeded_counts,
            'failed': self.interval_failed_counts,
            'batch_time': self.interval_batch_timings,
            'completion_rate': self.interval_completion_rates
        }


    @property
    def types_map(self) -> Dict[str, MetricType]:
        return {
            'completed': MetricType.COUNT,
            'succeeded': MetricType.COUNT,
            'failed': MetricType.COUNT,
            'batch_time': MetricType.SAMPLE,
            'completion_rate': MetricType.RATE
        }

    @property
    def grouped(self) -> Dict[str, Dict[str, Union[int, float]]]:

        
        grouped: Dict[str, Dict[str, Union[int, float]]] = {}

        for group_name, group_stats in self.stats.items():

            metrics: Dict[str, Union[int, float]] = {
                stat_name: calculation_method(
                    group_stats
                ) for stat_name, calculation_method in self._stat_types.items()
            }

            metrics.update(
                self._calculate_quantiles(group_stats)
            )

            grouped[group_name] = metrics

        return grouped

    @property
    def record(self) -> Dict[str, Union[int, float]]:

        record_stats: Dict[str, Dict[str, Union[int, float]]] = {}

        for group_name, group_stats in self.stats.items():
            for stat_name, calculation_method in self._stat_types.items():
                record_key = f'{stat_name}_{group_name}'

                record_stats[record_key] = calculation_method(group_stats)


        median_batch_time = record_stats['median_batch_time']
        if median_batch_time == 0:
            median_batch_time = 1

        actions_per_second = record_stats['median_completed']/median_batch_time
        actions_per_second_succeeded = record_stats['median_succeeded']/median_batch_time
        actions_per_second_failed = record_stats['median_failed']/median_batch_time

        record_stats.update({
            'actions_per_second': actions_per_second,
            'actions_per_second_succeeded': actions_per_second_succeeded,
            'actions_per_second_failed': actions_per_second_failed
        })

        return record_stats
    
    def _calculate_mean(self, stats:  List[Union[int, float]]) -> float:
        return statistics.mean(stats)

    def _calculate_median(self, stats: List[Union[int, float]]) -> Union[int, float]:
        return statistics.median(stats)

    def _calculate_min(self, stats:List[Union[int, float]]) -> Union[int, float]:
        return min(stats)
    
    def _calculate_max(self, stats: List[Union[int, float]]) -> Union[int ,float]:
        return max(stats)
    
    def _calculate_stdev(self, stats: List[Union[int, float]]):
        return statistics.stdev(stats)
    
    def _calculate_var(self, stats: List[Union[int, float]]) -> Union[int, float]:
        return statistics.variance(stats)
    
    def _calculate_quantiles(self, stats: List[Union[int, float]]) -> Dict[str, Union[int, float]]:
        
        quantile_stats: Dict[str, Union[int, float]] = {}

        for quantile in self.quantiles:
            stat_key = f'quantile_{quantile}th'

            quantile_stats[stat_key] = numpy.quantile(
                stats,
                round(
                    quantile/100,
                    2
                )
            )

        return quantile_stats
