import psutil
import functools
import inspect
from typing import Dict, List, Union
from hedra.core.hooks.types.hook import Hook
from hedra.core.hooks.types.types import HookType
from hedra.core.hooks.client.config import Config
from hedra.core.engines.types.common.request import Request
from hedra.core.engines.types.playwright.command import Command
from hedra.core.hooks.registry.registrar import registar
from hedra.core.pipelines.stages.types.stage_types import StageTypes
from hedra.core.personas import get_persona
from .execute import Execute
from .stage import Stage


class Setup(Stage):
    stage_type=StageTypes.SETUP
    log_level='info'
    persona_type='simple'
    total_time='1m'
    batch_size=1000
    batch_interval=1
    batch_gradient=0.1
    optimize_iterations=0
    optimizer_type='shg'
    cpus=psutil.cpu_count(logical=False)
    no_run_visuals=False
    connect_timeout=5
    request_timeout=60
    options={
        
    }
    
    def __init__(self) -> None:
        super().__init__()
        self.actions = []
        self.hooks: Dict[str, List[Hook]] = {}

        for hook_type in HookType:
            self.hooks[hook_type] = []

    async def run(self):
        execute_stages: Dict[str, Execute] = self.context.stages.get(StageTypes.EXECUTE)
        visited = self.context.setup
        setup_stages = {
            stage_name: stage for stage_name, stage in execute_stages.items() if stage_name not in visited
        }

        config = Config(
            log_level=self.log_level,
            persona_type=self.persona_type,
            total_time=self.total_time,
            batch_size=self.batch_size,
            batch_interval=self.batch_interval,
            batch_gradient=self.batch_gradient,
            optimize_iterations=self.optimize_iterations,
            optimizer_type=self.optimizer_type,
            cpus=self.cpus,
            no_run_visuals=self.no_run_visuals,
            connect_timeout=self.connect_timeout,
            request_timeout=self.request_timeout,
            options=self.options

        )

        persona = get_persona(config)
        
        for execute_stage_name, execute_stage in setup_stages.items():
            

            execute_stage.hooks = {
                hook_type: [] for hook_type in  HookType
            }

            execute_stage.client._config = config

            methods = inspect.getmembers(execute_stage, predicate=inspect.ismethod) 

            for _, method in methods:

                method_name = method.__qualname__
                hook: Hook = registar.all.get(method_name)
                
                if hook and execute_stage.hooks.get(hook.hook_type) is None:
                    execute_stage.hooks[hook.hook_type] = [hook]
                
                elif hook:
                    execute_stage.hooks[hook.hook_type].append(hook)
          
            for hook in execute_stage.hooks.get(HookType.ACTION):
                hook.name = hook.call.__name__
                execute_stage.client.next_name = hook.name
                session = await hook.call(execute_stage)
    
                session.context.history.add_row(
                    hook.name
                )

                parsed_action = session.registered.get(hook.name)

                parsed_action.hooks.before = self.get_hook(execute_stage, parsed_action, HookType.BEFORE)
                parsed_action.hooks.after = self.get_hook(execute_stage, parsed_action, HookType.AFTER)

                hook.session = session
                hook.action = parsed_action          


            for setup_hook in execute_stage.hooks.get(HookType.SETUP):
                await setup_hook.call(execute_stage)

            persona.setup(execute_stage.hooks)
            execute_stage.persona = persona    

            execute_stages[execute_stage_name] = execute_stage
        
        self.context.stages[StageTypes.EXECUTE] = execute_stages
        self.context.setup.extend(
            list(setup_stages.keys())
        )

    def get_hook(self, execute_stage: Execute, action: Union[Request,Command], hook_type: str):
        for hook in execute_stage.hooks[hook_type]:
            if action.name in hook.names:
                return functools.partial(hook.call, execute_stage)


    async def setup(self):
        for setup_hook in self.hooks.get(HookType.SETUP):
            await setup_hook()