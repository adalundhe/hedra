import asyncio
import time
from typing import Union
from easy_logger import Logger
from concurrent.futures import ProcessPoolExecutor
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.pipelines.hooks.types.hook import Hook
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.hooks.registry.registrar import registrar
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.reporting.events.types import (
    HTTPEvent, 
    HTTP2Event, 
    GraphQLEvent, 
    GRPCEvent, 
    WebsocketEvent, 
    PlaywrightEvent
)
from hedra.reporting.events import EventsGroup
from .parallel.analyze_stage_results import analyze_stage_results
from .stage import Stage


Events = Union[HTTPEvent, HTTP2Event, GraphQLEvent, GRPCEvent, WebsocketEvent, PlaywrightEvent]


class Analyze(Stage):
    stage_type=StageTypes.ANALYZE
    is_parallel = False
    handler = None

    def __init__(self) -> None:
        super().__init__()
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self.raw_results = {}
        self.quantile_ranges = [ 
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

        self.accepted_hook_types = [ HookType.METRIC ]
        self.requires_shutdown = True

    @Internal
    async def run(self):

        summaries = {
            'session_total': 0,
            'stages': {}
        }
        start = time.time()
        
        loop = asyncio.get_running_loop()

        metric_hook_names = [hook.name for hook in self.hooks.get(HookType.METRIC)]

        with ProcessPoolExecutor(max_workers=self.workers) as executor:
    
            processed_results = await asyncio.gather(*[
                loop.run_in_executor(
                    executor,
                    analyze_stage_results,
                    stage_name,
                    metric_hook_names,
                    stage_results
                ) for stage_name, stage_results in self.raw_results.items()
            ])

        for result in processed_results:
            summaries['stages'].update(result.get('stage_metrics'))
            summaries['session_total'] += result.get('stage_total')
        
        stop = time.time()
        print('TOOK: ', stop-start)

        self._shutdown_task = loop.run_in_executor(
            None,
            executor.shutdown
        )
                    
        return summaries
