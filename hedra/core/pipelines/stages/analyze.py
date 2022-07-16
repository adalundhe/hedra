from typing import Union
from easy_logger import Logger
from alive_progress import alive_bar
# from hedra.reporting import Handler, ParallelHandler
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage


class Analyze(Stage):
    stage_type=StageTypes.ANALYZE
    is_parallel = False
    handler = None

    def __init__(self) -> None:
        super().__init__()
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')
        
    async def gather(self):
        actions_per_second = self.stats.get('actions_per_second')
        completed_actions = self.stats.get('completed_actions')
        total_time = self.stats.get('total_time')
        
        if self.is_parallel is False:

            self.session_logger.info('\n')
            self.session_logger.info(f'Calculated APS of - {actions_per_second} - actions per second.')
            self.session_logger.info(f'Total action completed - {completed_actions} over actual runtime of - {total_time} - seconds.')
            self.session_logger.info('\n')

            self.session_logger.info(f'Processing - {completed_actions} - action results.')

            with alive_bar(
                total=self.stats.get('completed_actions'),
                title='Processing results...',
                bar=None, 
                spinner='dots_waves2'
            ) as bar:
                self.results = await self.handler.aggregate(self.results, bar=bar)

        else:
            self.results = await self.handler.aggregate(self.results)
