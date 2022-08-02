import asyncio
from easy_logger import Logger
from .optimizers import Optimizer
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from .stage import Stage


class Optimize(Stage):
    stage_type=StageTypes.OPTIMIZE
    optimize_iterations=0
    optimizer_type='shg'
    
    def __init__(self) -> None:
        super().__init__()
        self.persona = None
        self.results = None

    async def run(self):
        
        optmizer = Optimizer({
            'iterations': self.optimize_iterations,
            'algorithm': self.optimizer_type,
            'persona': self.persona
       
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

