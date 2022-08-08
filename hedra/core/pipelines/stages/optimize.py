import asyncio
from easy_logger import Logger
from .optimizers import Optimizer
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.engines.client.time_parser import TimeParser
from .stage import Stage


class Optimize(Stage):
    stage_type=StageTypes.OPTIMIZE
    optimize_iterations=0
    optimizer_type='shg'
    optimize_time='1m'
    
    def __init__(self) -> None:
        super().__init__()
        self.persona = None
        self.results = None

        time_parser = TimeParser(self.optimize_time)
        self.time_limit = time_parser.time

    @Internal
    async def run(self):
        
        optmizer = Optimizer({
            'iterations': self.optimize_iterations,
            'algorithm': self.optimizer_type,
            'persona': self.persona,
            'time_limit': self.time_limit
        })

        self.results = await optmizer.optimize()
        optimized_batch_size = self.results.get('optimized_batch_size')

        self.persona.batch.size = optimized_batch_size
        
        for hook in self.persona._hooks:
            hook.session.concurrency = optimized_batch_size
            hook.session.pool.size = optimized_batch_size
            hook.session.sem = asyncio.Semaphore(optimized_batch_size)
            hook.session.pool.connections = []
            hook.session.pool.create_pool()

        return self.persona

