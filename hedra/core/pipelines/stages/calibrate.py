from .warmup import Warmup
from alive_progress import alive_bar
from async_tools.datatypes import AsyncList


class Calibrate(Warmup):

    def __init__(self, config, persona) -> None:
        super().__init__(config, persona)
        self.order = 3

    async def execute(self, persona):
        self.selected_persona = persona
        self.selected_persona.duration = 60
        calibration_results_completed = 0

        if self.selected_persona.is_parallel is False:
            self.session_logger.info(f'Running post-optimize calibration for - {self.duration} seconds...')

        if self._no_run_visuals is False and self.selected_persona.is_parallel is False:

            with alive_bar(title='Calibrating', bar=None, spinner='dots_waves2', monitor=False, stats=False) as bar:
                calibration_results = await self.selected_persona.execute()
                calibration_results = AsyncList(calibration_results)
                calibration_results_completed = await calibration_results.size()

        else:
            calibration_results = await self.selected_persona.execute()
            calibration_results = AsyncList(calibration_results)
            calibration_results_completed = await calibration_results.size()

        if self.selected_persona.is_parallel is False:
            self.session_logger.info(
                f'\nCalibration complete - finished {calibration_results_completed} - actions over - {self.duration} - seconds.\n'
            )

        self.selected_persona.duration = self.total_time

        return self.selected_persona