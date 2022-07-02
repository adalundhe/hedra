import traceback
from alive_progress import alive_bar
from .base_stage import BaseStage


class Execution(BaseStage):

    def __init__(self, config, persona) -> None:
        super(Execution, self).__init__(config, persona)
        self.order = 4
        self.name = b'execute'
        self.results = []
        self.stats = {}


    @classmethod
    def about(cls):
        return '''
        Execution Stage


        The execution stage is the stage at which Hedra performes testing, records timing measurements, and otherwise
        performs actual testing. Execution occurs after setup, warmup, and optimization.
        
        '''

    async def execute(self, persona):
        self.selected_persona = persona

        try:
            if self._is_parallel is False:
                self.session_logger.info('Running load testing...')

            if self._no_run_visuals is False and self._is_parallel is False:
                with alive_bar(title='Executing test', bar=None, spinner='arrows_in', monitor=False, stats=False) as bar:  
                    self.results = await self.selected_persona.execute()

            else:
                self.results = await self.selected_persona.execute()

            completed_actions = self.selected_persona.total_actions
            total_time = self.selected_persona.total_elapsed
            actions_per_second = completed_actions / total_time

            if self._is_parallel is False:
                self.session_logger.debug(f'\nCompleted actions at - {actions_per_second} - actions per second')
                self.session_logger.debug(f'Total actions completed - {completed_actions} - over - {total_time} - seconds')


            self.selected_persona.results = list(self.results)
            self.selected_persona.stats = {
                'actions_per_second': actions_per_second,
                'completed_actions': self.selected_persona.total_actions,
                'total_time': self.selected_persona.total_elapsed,
                'end_time': self.selected_persona.end,
                'start_time': self.selected_persona.start
            }

        except Exception as e:
            print(traceback.format_exc())
            pass

        return self.selected_persona