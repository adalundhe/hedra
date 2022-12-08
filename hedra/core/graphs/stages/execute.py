from ctypes import Union
import dill
import statistics
from time import sleep
from typing import Dict, Generic
from hedra.core.engines.client import Client
from typing_extensions import TypeVarTuple, Unpack
from hedra.core.engines.types.registry import engines_registry
from hedra.core.graphs.hooks.types.types import HookType
from hedra.core.graphs.hooks.types.internal import Internal
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.personas.persona_manager import get_persona, registered_personas
from hedra.plugins.types.plugin_types import PluginType

from .parallel.types import PartitionMethod
from .parallel.execute_actions import execute_actions
from .stage import Stage

T = TypeVarTuple('T')

class Execute(Stage, Generic[Unpack[T]]):
    stage_type=StageTypes.EXECUTE

    def __init__(self) -> None:
        super().__init__()
        self.persona = None
        self.client: Client[Unpack[T]] = Client()
        
        self.accepted_hook_types = [ 
            HookType.SETUP, 
            HookType.BEFORE, 
            HookType.ACTION,
            HookType.TASK,
            HookType.AFTER,
            HookType.TEARDOWN,
            HookType.CHECK
        ]

        self.concurrent_pool_aware_stages = 0
        self.execution_stage_id = 0
        self.optimized = False
        self.execute_setup_stage = None
        self.requires_shutdown = True
        self.allow_parallel = True

    @Internal
    async def run(self):

        engine_plugins = self.plugins_by_type.get(PluginType.ENGINE)
        for plugin in engine_plugins.values():
            engines_registry[plugin.name] = plugin

        persona_plugins = self.plugins_by_type.get(PluginType.PERSONA)
        for plugin_name, plugin in persona_plugins.items():
            registered_personas[plugin_name] = lambda config: plugin(config)

        if self.workers > 1:

            hooks = [
                {
                    'timeouts': hook.session.timeouts,
                    'reset_connections': hook.session.pool.reset_connections,
                    'hook_name': hook.name,
                    'stage': hook.stage,
                    'weight': hook.config.weight,
                    'order': hook.config.order,
                    **hook.action.to_serializable()
                } for hook in self.hooks.get(HookType.ACTION, [])
            ]

            hooks.extend([
                {
                    'timeouts': hook.session.timeouts,
                    'reset_connections': False,
                    'hook_name': hook.name,
                    'stage': hook.stage,
                    'weight': hook.config.weight,
                    'order': hook.config.order,
                    **hook.action.to_serializable()
                } for hook in self.hooks.get(HookType.TASK, [])
            ])             
    
            results_sets = await self.executor.execute_stage_batch(
                execute_actions,
                [
                    dill.dumps({
                        'partition_method': PartitionMethod.BATCHES,
                        'workers': self.workers,
                        'worker_id': idx + 1,
                        'config': self.client._config,
                        'hooks': hooks
                    }) for idx in range(self.executor.max_workers)
                ]
            )
            
            results = []
            elapsed_times = []
            for result_set in results_sets:
                results.extend(result_set.get('results'))
                elapsed_times.append(result_set.get('total_elapsed'))

            total_elapsed = statistics.median(elapsed_times)

            print(len(results))
            print(len(results)/total_elapsed)

        else:

            persona = get_persona(self.client._config)
            persona.setup(self.hooks)

            results = await persona.execute()
            total_elapsed = persona.total_elapsed

        return {
            'results': results,
            'total_elapsed': total_elapsed
        }