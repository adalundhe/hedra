import asyncio
import psutil
import functools
import inspect
from typing import Dict, List
from hedra.core.hooks.client.client import Client
from hedra.core.hooks.types.hook import Hook
from hedra.core.hooks.types.types import HookType
from hedra.core.hooks.client.config import Config
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
    cpus=psutil.cpu_count(logical=False)
    no_run_visuals=False
    connect_timeout=5
    request_timeout=60
    reporting_config={}
    options={}
    
    def __init__(self) -> None:
        super().__init__()
        self.stages = {}
        self.actions = []
        self.hooks: Dict[str, List[Hook]] = {}

        for hook_type in HookType:
            self.hooks[hook_type] = []

    async def run(self):

        config = Config(
            log_level=self.log_level,
            persona_type=self.persona_type,
            total_time=self.total_time,
            batch_size=self.batch_size,
            batch_interval=self.batch_interval,
            batch_gradient=self.batch_gradient,
            cpus=self.cpus,
            no_run_visuals=self.no_run_visuals,
            connect_timeout=self.connect_timeout,
            request_timeout=self.request_timeout,
            options=self.options

        )

        methods = inspect.getmembers(self, predicate=inspect.ismethod) 
        for _, method in methods:

            method_name = method.__qualname__
            hook: Hook = registar.all.get(method_name)

            if hook:
                self.hooks[hook.hook_type].append(hook)
        
        await asyncio.gather(*[hook.call() for hook in self.hooks.get(HookType.ACTION)])

        
        for execute_stage_name, execute_stage in self.stages.items():
            persona = get_persona(config)
            execute_stage.hooks = {
                hook_type: [] for hook_type in  HookType
            }

            client = Client()
            execute_stage.client = client

            execute_stage.client._config = config

            methods = inspect.getmembers(execute_stage, predicate=inspect.ismethod) 

            for _, method in methods:

                method_name = method.__qualname__
                hook: Hook = registar.all.get(method_name)
                
                if hook:
                    execute_stage.hooks[hook.hook_type].append(hook)
            
            for hook in execute_stage.hooks.get(HookType.ACTION):
                execute_stage.client.next_name = hook.name
                session = await hook.call()

                session.context.history.add_row(
                    hook.name
                )

                parsed_action = session.registered.get(hook.name)

                parsed_action.hooks.before = self.get_hook(execute_stage, hook.shortname, HookType.BEFORE)
                parsed_action.hooks.after = self.get_hook(execute_stage, hook.shortname, HookType.AFTER)
                parsed_action.hooks.checks = self.get_checks(execute_stage, hook.shortname)

                hook.session = session
                hook.action = parsed_action          


            for setup_hook in execute_stage.hooks.get(HookType.SETUP):
                await setup_hook.call()

            persona.setup(execute_stage.hooks)
            execute_stage.persona = persona    

            self.stages[execute_stage_name] = execute_stage

        return self.stages

    def get_hook(self, execute_stage: Execute, shortname: str, hook_type: str):
        for hook in execute_stage.hooks[hook_type]:
            if shortname in hook.names:
                return hook.call

    def get_checks(self, execute_stage: Execute, shortname: str):

        checks = []

        for hook in execute_stage.hooks[HookType.CHECK]:
            if shortname in hook.names:
                checks.append(hook.call)

        return checks

    async def setup(self):
        for setup_hook in self.hooks.get(HookType.SETUP):
            await setup_hook()