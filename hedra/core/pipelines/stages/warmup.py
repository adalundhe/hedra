from alive_progress import alive_bar
from async_tools.datatypes import AsyncList
from .base_stage import BaseStage


class Warmup(BaseStage):

    def __init__(self, config, persona) -> None:
        super(Warmup, self).__init__(config, persona)

        self.order = 2
        self.name = b'warmup'
        self.duration = config.executor_config.get('warmup', 0)
        self.execute_stage = self.duration > 0
        self._total_time = None

        if self.duration > 60:
            self.duration = 60

    @classmethod
    def about(cls):
        return '''
        Warmup Stage

        key-arguments:

        --warmup <warmup_duation_seconds>

        The warmup stage is an (optional) stage that allows for a pre-execution run of the specified persona/engine. The 
        warmup stage is useful for both helping target instances scale-up/acclimate to tests prior to actual 
        measurement/execution as well as helping the specified engine perform initial caching that may assist
        in performance of large batch sizes or complex tests.

        '''

    async def execute(self, persona):
        self.selected_persona = persona
        self._total_time = persona.total_time
        self.selected_persona.total_time = self.duration
        warmup_results_completed = 0

        if self.selected_persona.is_parallel is False:
            self.session_logger.info(f'Running pre-test warmup for - {self.duration} seconds...')

        if self._no_run_visuals is False and self.selected_persona.is_parallel is False:

            with alive_bar(title='Warming up', bar=None, spinner='dots_waves2', monitor=False, stats=False) as bar:
                warmup_results = await self.selected_persona.execute()
                warmup_results = AsyncList(warmup_results)
                warmup_results_completed = await warmup_results.size()

        else:
            warmup_results = await self.selected_persona.execute()
            warmup_results = AsyncList(warmup_results)
            warmup_results_completed = await warmup_results.size()

        if self.selected_persona.is_parallel is False:
            self.session_logger.info(
                f'\nWarmup complete - finished {warmup_results_completed} - actions over - {self.duration} - seconds.\n'
            )

        self.selected_persona.total_time = self._total_time

        return self.selected_persona