from __future__ import annotations
import uuid
import numpy
from collections import defaultdict
from typing import Any, Dict, Union
from hedra.logging import HedraLogger
from hedra.reporting.stats import (
    Median,
    Mean,
    Variance,
    StandardDeviation
)
from .results import results_types
from .types.task_event import TaskEvent
from .types.base_event import BaseEvent


class EventsGroup:

    __slots__ = (
        'events_group_id',
        'source',
        'groups',
        'events',
        'timings',
        'tags',
        'total',
        'succeeded',
        'failed',
        'errors',
        '_streaming_mean',
        '_streaming_variance',
        '_streaming_stdev',
        '_streaming_median'
    )

    def __init__(self) -> None:

        self.events_group_id = str(uuid.uuid4())

        self.groups: Dict[Dict[str, Union[int, float]]] = {}
        self.timings = defaultdict(list)
        self.source = None
        self.tags = {}
        self.total = 0
        self.succeeded = 0
        self.failed = 0
        self.errors = defaultdict(lambda: 0)

        self._streaming_mean = defaultdict(Mean)
        self._streaming_variance = defaultdict(Variance)
        self._streaming_stdev = defaultdict(StandardDeviation)
        self._streaming_median = defaultdict(Median)

    async def add(self, result: Any, stage_name: str):

        event: BaseEvent = results_types.get(result.type, TaskEvent)(result)
        event.stage = stage_name

        if self.source is None:
            self.source = event.source

        self.tags.update(event.tags_to_dict())
    
        await event.check_result()

        if event.error is None:
            self.succeeded += 1
        
        else:
            self.errors[event.error] += 1
            self.failed += 1

        for timing_group, timing in event.timings.items():
            if timing > 0:
                self.timings[timing_group].append(timing)

    def calculate_partial_group_stats(self):

        self.total = self.succeeded + self.failed

        for group_name, group_timings in self.timings.items():

            if len(group_timings) == 0:
                group_timings = [0]

            median = float((numpy.median(group_timings)))
            mean = float(numpy.mean(group_timings, dtype=numpy.float64))
            variance = float(numpy.var(group_timings, dtype=numpy.float64, ddof=1))
            stdev = float(numpy.std(group_timings, dtype=numpy.float64))

            self._streaming_median[group_name].update(median)
            self._streaming_mean[group_name].update(mean)
            self._streaming_variance[group_name].update(variance)
            self._streaming_stdev[group_name].update(stdev)

            self.groups[group_name] = {
                group_name: {
                    'median': median,
                    'mean': mean,
                    'variance': variance,
                    'stdev': stdev,
                    'minimum': min(group_timings),
                    'maximum': max(group_timings)
                }

            }

    def calculate_quantiles(self):

        quantile_ranges = [ 
            .10, 
            .20, 
            .25, 
            .30, 
            .40, 
            .50, 
            .60, 
            .70, 
            .75, 
            .80, 
            .90, 
            .95, 
            .99 
        ]

        for group_name, group_timings in self.timings.items():

            if len(group_timings) == 0:
                group_timings = [0]

            quantiles = {
                f'quantile_{int(quantile_range * 100)}th': quantile for quantile, quantile_range in zip(
                    numpy.quantile(
                        numpy.array(group_timings), quantile_ranges),
                    quantile_ranges
                )
            }
            
            self.groups[group_name]['quanties'] = quantiles

    def merge(self, group: EventsGroup):
        
        self.tags.update(group.tags)

        self.total += group.total
        self.succeeded += group.succeeded
        self.failed += group.failed

        for error, error_count in group.errors.items():
            self.errors[error] += error_count

        for group_name in group.timings.keys():
            self.timings[group_name].extend(group.timings[group_name])

            merge_group_stats = group.groups.get(
                group_name, 
                {}
            ).get(group_name)

            stats_group = self.groups.get(
                group_name,
                {}
            ).get(group_name)


            update_median = group._streaming_median[group_name].get()
            update_mean = group._streaming_mean[group_name].get()
            update_variance = group._streaming_variance[group_name].get()
            updated_stdev = group._streaming_stdev[group_name].get()

            if self.groups.get(group_name) is None:
                self.groups[group_name] = {
                    group_name: {
                        'median': update_median,
                        'mean': update_mean,
                        'variance': update_variance,
                        'stdev': updated_stdev,
                        'minimum': merge_group_stats['minimum'],
                        'maximum': merge_group_stats['maximum']
                    }
                }

            else:
                
                self._streaming_median[group_name].update(update_median)
                self._streaming_mean[group_name].update(update_mean)
                self._streaming_variance[group_name].update(update_variance)
                self._streaming_stdev[group_name].update(updated_stdev)

                self.groups[group_name] = {
                    group_name: {
                        'median': self._streaming_median[group_name].get(),
                        'mean': self._streaming_mean[group_name].get(),
                        'variance': self._streaming_variance[group_name].get(),
                        'stdev': self._streaming_stdev[group_name].get(),
                        'minimum': min([
                            merge_group_stats['minimum'],
                            stats_group['minimum']
                        ]),
                        'maximum': max([
                            merge_group_stats['maximum'],
                            stats_group['maximum']
                        ])
                    }
                }
