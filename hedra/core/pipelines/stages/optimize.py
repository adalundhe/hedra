import asyncio

from hedra.core.pipelines.hooks.types.types import HookType
from hedra.core.pipelines.hooks.types.hook import Hook
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.pipelines.hooks.types.internal import Internal
from hedra.core.engines.client.time_parser import TimeParser
from hedra.core.engines.client.config import Config
from hedra.core.personas import get_persona
from .stage import Stage
from .optimizers import Optimizer


class Optimize(Stage):
    stage_type=StageTypes.OPTIMIZE
    optimize_iterations=0
    optimizer_type='shg'
    optimize_time='1m'
    
    def __init__(self) -> None:
        super().__init__()
        self.execute_stage_config: Config = None
        self.execute_stage_hooks: Hook = []
        self.concurrent_execution_stages = []
        self.execution_stage_id = 0

        self.results = None

        time_parser = TimeParser(self.optimize_time)
        self.time_limit = time_parser.time

    @Internal
    async def run(self):

        config = self.execute_stage_config
        
        batch_size = config.batch_size
        stages_count = len(self.concurrent_execution_stages)
        
        if stages_count > 0 and self.execution_stage_id == stages_count:
            batch_size = int(batch_size/stages_count) + batch_size%stages_count

        elif stages_count > 0:
            batch_size = int(batch_size/stages_count)

        config.batch_size = batch_size

        persona = get_persona(config)
        persona.setup(self.execute_stage_hooks)

        
        optmizer = Optimizer({
            'iterations': self.optimize_iterations,
            'algorithm': self.optimizer_type,
            'persona': persona,
            'time_limit': self.time_limit
        })

        self.results = await optmizer.optimize()
        optimized_batch_size = self.results.get('optimized_batch_size')

        config.batch_size = optimized_batch_size
        
        for hook in self.execute_stage_hooks.get(HookType.ACTION):
            hook.session.concurrency = optimized_batch_size
            hook.session.pool.size = optimized_batch_size
            hook.session.sem = asyncio.Semaphore(optimized_batch_size)
            hook.session.pool.connections = []
            hook.session.pool.create_pool()

        return config, self.execute_stage_hooks

