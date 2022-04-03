from .base_stage import BaseStage
from hedra.core.optimizers import Optimizer
from .calibrate import Calibrate


class Optimize(BaseStage):
    
    def __init__(self, config, persona) -> None:
        super(Optimize, self).__init__(config, persona)

        self.calibration = Calibrate(config, persona)
        self.order = 3
        self.name = b'optimize'
        self.iters = config.executor_config.get('optimize', 0)
        self.execute_stage = self.iters > 0
        self.optmizer = None

        if self.execute_stage:
            self.optmizer = Optimizer(self.selected_persona)

        self.optimized_params = None

    @classmethod
    def about(cls):
        return '''
        Optimize Stage

        The optimize stage is an (optional) stage that occurs if the --optimize argument is provided. It 
        occurs after warmup but pre-execution, passing the optimized batch size and batch time to the subsequent
        execution stage for use.

        '''

    async def execute(self, persona):

        if self._is_parallel is False:
            self.session_logger.info('Estimating batch size and batch time...')
            
        self.selected_persona = persona
        self.selected_persona.optimized_params = await self.optmizer.optimize()
        optimized_batch_size = self.selected_persona.optimized_params['optimized_batch_size']
        optimized_batch_time = self.selected_persona.optimized_params['optimized_batch_time']

        optimization_iters = self.selected_persona.optimized_params['optimization_iters']
        optimization_total_time = self.selected_persona.optimized_params['optimization_total_time']
        max_actions_completed = self.selected_persona.optimized_params['max_actions_completed']

        if self._is_parallel is False:
            self.session_logger.info('\nOptimization complete...')
            self.session_logger.info(f'Executed - {optimization_iters} - iterations over - {optimization_total_time} - seconds.')
            self.session_logger.info(f'Best batch size - {optimized_batch_size}')
            self.session_logger.info(f'Best batch time - {optimized_batch_time}')
            self.session_logger.info(f'Highest actions per second (APS) - {max_actions_completed/optimized_batch_time}.')
            self.session_logger.info('\n')

        self.selected_persona.batch.size = optimized_batch_size
        self.selected_persona.batch.time = optimized_batch_time
        await self.selected_persona.load_batches()

        return await self.calibration.execute(self.selected_persona)
