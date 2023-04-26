import numpy
from typing import Union, List, Dict
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.task.hook import TaskHook
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.versioning.flags.types.unstable.flag import unstable


@unstable
class Stream:

    def __init__(self) -> None:
        self.completed_count = 0
        self.last_completed = 0
        self.last_batch_size = 0
        self.action = None
        self.completed: List[BaseResult] = []

    @property
    def succeeded(self) -> int:
        return len([
            result for result in self.completed if result.error is None
        ])
    
    @property
    def failed(self) -> int:
        succeeded = self.succeeded
        return len(self.completed) - succeeded
    
    @property
    def timings(self) -> List[Dict[str, float]]:

        timings = {
            'total': [],
            'waiting': [],
            'connecting': [],
            'writing': [],
            'reading': []
        }

        for result in self.completed:
            timings['total'].append(result.complete - result.start)
            timings['waiting'].append(result.start - result.wait_start)
            timings['connecting'].append(result.connect_end - result.start)
            timings['writing'].append(result.write_end - result.connect_end)
            timings['reading'].append(result.complete - result.write_end)

        stream_timings = {}

        for timing_group_name, timing_group in timings.items():
            if len(timing_group) > 0:
                stream_timings[timing_group_name] = {
                    'maximum': max(timing_group),
                    'minimum': min(timing_group),
                    'median': numpy.median(timing_group),
                    'mean': numpy.mean(timing_group),
                    'stdev': numpy.std(timing_group),
                    'variance': numpy.var(timing_group)
                }

            else:
                stream_timings[timing_group_name] = {
                    'maximum': 0,
                    'minimum': 0,
                    'median': 0,
                    'mean': 0,
                    'stdev': 0,
                    'variance': 0
                }



        return stream_timings


    async def execute_action(self, hook: Union[ActionHook, TaskHook]):
        try:
            result: BaseResult = await hook.session.execute_prepared_request(
                hook.action
            )
        except RuntimeError as runtime_error:
            result = runtime_error
        
        self.completed_count += 1
        self.completed.append(result)

        return result