from easy_logger import Logger
from hedra.core.optimizers import Optimizer
from hedra.test.config import Config
from hedra.test.stages.types.stage_types import StageTypes
from .stage import Stage


class Optimize(Stage):
    stage_type=StageTypes.OPTIMIZE
    stage_order=2
    config: Config = None
    is_parallel = False
    
    def __init__(self) -> None:
        super().__init__()
        self.order = 3
        self.name = b'optimize'
        self.iters = self.config.optimize
        self.execute_stage = self.iters > 0
        self.optmizer = None
        self.selected_persona = None

        self.optmizer = Optimizer(self.selected_persona)
        self.optimized_params = None
        
        logger = Logger()
        self.session_logger = logger.generate_logger('hedra')

    async def optimize(self, persona):

        if self.is_parallel is False:
            self.session_logger.info('Estimating batch size and batch time...')
            
        self.selected_persona = persona
        self.selected_persona.optimized_params = await self.optmizer.optimize()
        optimized_batch_size = self.selected_persona.optimized_params['optimized_batch_size']
        optimized_batch_interval = self.selected_persona.optimized_params['optimized_batch_interval']

        optimization_iters = self.selected_persona.optimized_params['optimization_iters']
        optimization_total_time = self.selected_persona.optimized_params['optimization_total_time']
        optimization_max_aps = self.selected_persona.optimized_params['optimization_max_aps']

        if self.is_parallel is False:
            self.session_logger.info('\nOptimization complete...')
            self.session_logger.info(f'Executed - {optimization_iters} - iterations over - {optimization_total_time} - seconds.')
            self.session_logger.info(f'Best batch size - {optimized_batch_size}')
            self.session_logger.info(f'Best batch interval - {optimized_batch_interval}')
            self.session_logger.info(f'Highest actions per second (APS) - {optimization_max_aps}.')
            self.session_logger.info('\n')
    
        self.selected_persona.batch.size = optimized_batch_size
        self.selected_persona.batch.interval.wait_period = optimized_batch_interval