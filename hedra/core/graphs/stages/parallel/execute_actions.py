import asyncio
import dill
from typing import Dict, Any
from hedra.core.engines.client.config import Config
from hedra.core.graphs.hooks.types.hook import Hook
from hedra.core.graphs.hooks.types.hook_types import HookType

from hedra.core.personas import get_persona
from .partition_method import PartitionMethod
from .action_assembly import ActionAssembler


def execute_actions(parallel_config: str):

    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        parallel_config: Dict[str, Any] = dill.loads(parallel_config)
        partition_method = parallel_config.get('partition_method')
        persona_config: Config = parallel_config.get('config')
        workers = parallel_config.get('workers')
        worker_id = parallel_config.get('worker_id')

        if partition_method == PartitionMethod.BATCHES and persona_config.optimized is False:
            if workers == worker_id:
                persona_config.batch_size = int(persona_config.batch_size/workers) + (persona_config.batch_size%workers)
            
            else:
                persona_config.batch_size = int(persona_config.batch_size/workers)

        persona = get_persona(persona_config)
        persona.workers = workers

        hooks = {
            HookType.ACTION: [],
            HookType.TASK: [],
        }


        for hook_action in parallel_config.get('hooks'):
            hook_type = hook_action.get('hook_type', HookType.ACTION)
            action_name = hook_action.get('name')

            hook = Hook(
                hook_action.get('hook_name'),
                action_name,
                None,
                hook_action.get('stage'),
                hook_type=HookType.ACTION

            )

            action_type = hook_action.get('type')
            action_assembler = ActionAssembler(
                hook,
                hook_action,
                persona
            )

            hooks[hook_type].append(action_assembler.assemble(action_type))
                
        persona.setup(hooks)

        results = loop.run_until_complete(persona.execute())

        return {
            'results': results,
            'total_results': len(results),
            'total_elapsed': persona.total_elapsed
        }
    except Exception as e:
        raise e