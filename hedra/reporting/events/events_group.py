from __future__ import annotations
import numpy
import statistics
from collections import defaultdict
from typing import Any, Dict, List, Union
from .results import results_types
numpy.seterr(over='raise')

def trunc(values, decs=0):
    return numpy.trunc(values*10**decs)/(10**decs)

class EventsGroup:

    __slots__ = (
        'source',
        'groups',
        'events',
        'timings',
        'tags',
        'total',
        'succeeded',
        'failed',
        'errors'
    )

    def __init__(self) -> None:
        self.groups: Dict[Dict[str, Union[int, float]]] = {}
        self.timings = defaultdict(list)
        self.source = None
        self.tags = {}
        self.total = 0
        self.succeeded = 0
        self.failed = 0
        self.errors = defaultdict(lambda: 0)

    async def add(self, result: Any, stage_name: str):

        event = results_types.get(result.type)(result)
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
        
        if event.time > 0:
            self.timings['total'].append(event.time)

        if event.time_waiting > 0:
            self.timings['waiting'].append(event.time_waiting)

        if event.time_connecting > 0:
            self.timings['connecting'].append(event.time_connecting)

        if event.time_writing > 0:
            self.timings['writing'].append(event.time_writing)

        if event.time_reading > 0:
            self.timings['reading'].append(event.time_reading)

    def calculate_partial_group_stats(self):
        self.total = self.succeeded + self.failed


        for group_name, group_timings in self.timings.items():

            if len(group_timings) == 0:
                group_timings = [0]

            self.groups[group_name] = {
                group_name: {
                    'median': float((numpy.median(group_timings))),
                    'mean': float(numpy.mean(group_timings, dtype=numpy.float64)),
                    'variance': float(numpy.var(group_timings, dtype=numpy.float64, ddof=1)),
                    'stdev': float(numpy.std(group_timings, dtype=numpy.float64)),
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

        self.timings['total'].extend(group.timings['total'])
        self.timings['waiting'].extend(group.timings['waiting'])
        self.timings['connecting'].extend(group.timings['connecting'])
        self.timings['writing'].extend(group.timings['writing'])
        self.timings['reading'].extend(group.timings['reading'])

        for group_name in self.timings.keys():
            merge_group_stats = group.groups.get(
                group_name, 
                {}
            ).get(group_name)

            stats_group = self.groups.get(
                group_name,
                {}
            ).get(
                group_name
            )

            if stats_group is None:
                self.groups[group_name] = {
                    group_name: merge_group_stats
                }

            else:

                self.groups[group_name] = {
                    group_name: {
                        'median': float((numpy.median([
                            merge_group_stats['median'],
                            stats_group['median']
                        ]))),
                        'mean': float(numpy.mean([
                            merge_group_stats['mean'],
                            stats_group['mean']
                        ])),
                        'variance': float(numpy.var(
                            trunc([
                                merge_group_stats['variance'],
                                stats_group['variance'],
                            ], decs=6)
                        )),
                        'stdev': float(numpy.std([
                            merge_group_stats['stdev'],
                            stats_group['stdev']
                        ])),
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
