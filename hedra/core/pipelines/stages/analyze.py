from typing import Union
from easy_logger import Logger
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.reporting.events.types import (
    HTTPEvent, 
    HTTP2Event, 
    GraphQLEvent, 
    GRPCEvent, 
    WebsocketEvent, 
    PlaywrightEvent,
    UDPEvent
)
from .parallel.batch_results import BatchedResults
from .parallel.analyze_results import analyze_results_batched
from .stage import Stage


Events = Union[HTTPEvent, HTTP2Event, GraphQLEvent, GRPCEvent, WebsocketEvent, PlaywrightEvent, UDPEvent]


class Analyze(Stage):
    stage_type=StageTypes.ANALYZE
    is_parallel = False
    handler = None

    def __init__(self) -> None:
        super().__init__()
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        self.raw_results = {}

        self.accepted_hook_types = [ HookType.METRIC ]
        self.requires_shutdown = True

    @Internal
    async def run(self):

        

        summaries = {
            'session_total': 0,
            'stages': {}
        }

        metric_hook_names = [hook.name for hook in self.hooks.get(HookType.METRIC)]

        batch_results = BatchedResults(
            list(self.raw_results.items())
        )

        batched_stages = batch_results.partion_stage_batches()

        processed_results = await batch_results.execute_batches(
            batched_stages,
            analyze_results_batched,
            custom_metric_hook_names=metric_hook_names
        )


        for result in processed_results:
            summaries['stages'].update(result.get('stage_metrics'))
            summaries['session_total'] += result.get('stage_total')
         
        return summaries
