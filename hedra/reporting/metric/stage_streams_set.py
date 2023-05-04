import statistics
import numpy
from hedra.core.personas.streaming.stream_analytics import StreamAnalytics
from typing import List, Dict, Union, Callable


CalculationMethod = Callable[[Dict[str,List[Union[int, float]]]], Dict[str, Union[int, float]]]


class StageStreamsSet:

    def __init__(self, stage_streams: List[StreamAnalytics]) -> None:
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
            'median': self._calculate_median_stats,
            'mean': self._calculate_mean_stats,
            'max': self._calculate_max_stats,
            'min': self._calculate_min_stats,
            'stdev': self._calculate_stdev_stats,
            'variance': self._calculate_var_stats,
            'quantiles': self._calculate_quantile_stats
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

    @property
    def record(self) -> Dict[str, Union[int, float]]:

        stats: Dict[str, List[Union[int, float]]] = {
            'completed': self.interval_completed_counts,
            'succeeded': self.interval_succeeded_counts,
            'failed': self.interval_failed_counts,
            'batch_time': self.interval_batch_timings,
            'completion_rate': self.interval_completion_rates
        }

        metrics = {}

        for calculation_method in self._stat_types.values():
            metrics.update(
                calculation_method(stats)
            )

        median_batch_time = metrics['median_batch_time']
        if median_batch_time == 0:
            median_batch_time = 1

        actions_per_second = metrics['median_completed']/median_batch_time
        actions_per_second_succeeded = metrics['median_succeeded']/median_batch_time
        actions_per_second_failed = metrics['median_failed']/median_batch_time

        metrics.update({
            'actions_per_second': actions_per_second,
            'actions_per_second_succeeded': actions_per_second_succeeded,
            'actions_per_second_failed': actions_per_second_failed
        })

        return metrics
    
    def _calculate_mean_stats(self, stats: Dict[str, List[Union[int, float]]]):
        
        mean_stats: Dict[str, Union[int, float]] = {}

        for stat_name, stat_values in stats.items():
            stat_key = f'mean_{stat_name}'

            mean_stats[stat_key] = statistics.mean(stat_values)

        return mean_stats

    def _calculate_median_stats(self, stats: Dict[str, List[Union[int, float]]]):
        
        median_stats: Dict[str, Union[int, float]] = {}

        for stat_name, stat_values in stats.items():
            stat_key = f'median_{stat_name}'

            median_stats[stat_key] = statistics.median(stat_values)

        return median_stats

    def _calculate_min_stats(self, stats: Dict[str, List[Union[int, float]]]):
        
        min_stats: Dict[str, Union[int, float]] = {}

        for stat_name, stat_values in stats.items():
            stat_key = f'min_{stat_name}'

            min_stats[stat_key] = min(stat_values)

        return min_stats
    
    def _calculate_max_stats(self, stats: Dict[str, List[Union[int, float]]]):
        
        max_stats: Dict[str, Union[int, float]] = {}

        for stat_name, stat_values in stats.items():
            stat_key = f'max_{stat_name}'

            max_stats[stat_key] = max(stat_values)

        return max_stats
    
    def _calculate_stdev_stats(self, stats: Dict[str, List[Union[int, float]]]):
        
        stdev_stats: Dict[str, Union[int, float]] = {}

        for stat_name, stat_values in stats.items():
            stat_key = f'stdev_{stat_name}'

            stdev_stats[stat_key] = statistics.stdev(stat_values)

        return stdev_stats
    
    def _calculate_var_stats(self, stats: Dict[str, List[Union[int, float]]]):
        
        var_stats: Dict[str, Union[int, float]] = {}

        for stat_name, stat_values in stats.items():
            stat_key = f'var_{stat_name}'

            var_stats[stat_key] = statistics.variance(stat_values)

        return var_stats
    
    def _calculate_quantile_stats(self, stats: Dict[str, List[Union[int, float]]]):
        
        quantile_stats: Dict[str, Union[int, float]] = {}

        for stat_name, stat_values in stats.items():
            for quantile in self.quantiles:
                stat_key = f'${quantile}th_quantile_{stat_name}'

            quantile_stats[stat_key] = numpy.quantile(
                stat_values,
                round(
                    quantile/100,
                    2
                )
            )

        return quantile_stats
