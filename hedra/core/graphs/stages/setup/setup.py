import asyncio
import psutil
import traceback
from collections import defaultdict
from typing_extensions import TypeVarTuple, Unpack
from typing import (
    Dict, 
    Generic, 
    List, 
    Any, 
    Optional, 
    Union
)
from hedra.core.experiments.experiment import Experiment
from hedra.core.engines.client.client import Client
from hedra.core.engines.client.config import Config
from hedra.core.engines.client.tracing_config import TracingConfig
from hedra.core.hooks.types.condition.decorator import condition
from hedra.core.hooks.types.context.decorator import context
from hedra.core.hooks.types.event.decorator import event
from hedra.core.hooks.types.transform.decorator import transform
from hedra.core.hooks.types.base.hook import Hook
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.internal.decorator import Internal
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.task.hook import TaskHook
from hedra.core.graphs.stages.base.parallel.stage_priority import StagePriority
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.personas.types import PersonaTypesMap
from hedra.logging import HedraLogger
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.persona.persona_plugin import PersonaPlugin
from hedra.plugins.types.plugin_types import PluginType
from hedra.core.graphs.stages.base.stage import Stage
from .exceptions import HookSetupError

try:

    from playwright.async_api import Geolocation

except Exception:
    Geolocation = None


T = TypeVarTuple('T')


class SetupCall:

    def __init__(self, hook: Hook, config: Config, retries: int=1) -> None:
        self.hook = hook
        self.config = config
        self.hook_name = self.hook.hook_type.name.capitalize()
        self.exception = None
        self.action_store = None
        self.retries = retries
        self.current_try = 1
        self.logger = HedraLogger()
        self.logger.initialize()
        self.metadata_string:str = None
        self.error_traceback: str = None

    async def setup(self):

        for _ in range(self.retries):
            try:

                self.hook.stage_instance.client._config = self.config
                self.hook.stage_instance.client.set_mutations()
                await self.hook.call()

            except Exception as setup_exception:
                self.exception = setup_exception

                self.error_traceback = str(traceback.format_exc())
                await self.logger.filesystem.aio['hedra.core'].error(f'{self.metadata_string} - Encountered connection validation error - {str(setup_exception)} - {self.hook_name} Hook: {self.hook.name}:{self.hook.hook_id}')

                if self.current_try >= self.retries:
                    self.action_store.waiter.set_result(None)
                    break

                else:
                    self.current_try += 1



class Setup(Stage, Generic[Unpack[T]]):
    experiment: Optional[Experiment] = None
    stage_type=StageTypes.SETUP
    log_level='info'
    persona_type='default'
    total_time='1m'
    batch_size=1000
    batch_interval=0
    action_interval=0
    batch_gradient=0.1
    cpus=int(psutil.cpu_count(logical=False))
    no_run_visuals=False
    graceful_stop=1
    connect_timeout=10
    request_timeout=60
    reset_connections=False
    apply_to_stages=[]
    browser_type: str='chromium'
    device_type: str=None
    locale: str=None
    geolocation: Geolocation=None
    permissions: List[str]=[]
    playwright_options: Dict[str, Any]={}
    tracing: TracingConfig=None
    priority: Optional[str]=None
    actions_filepaths: Optional[Dict[str, str]]=None

    
    def __init__(self) -> None:
        super().__init__()
        self.generation_setup_candidates = 0
        self.stages: Dict[str, Stage] = {}
        self.accepted_hook_types = [ 
            HookType.CONDITION,
            HookType.CONTEXT,
            HookType.EVENT, 
            HookType.TRANSFORM 
        ]

        self.persona_types = PersonaTypesMap()
        self.config = Config(
            log_level=self.log_level,
            persona_type=self.persona_types[self.persona_type],
            total_time=self.total_time,
            batch_size=self.batch_size,
            batch_interval=self.batch_interval,
            action_interval=self.action_interval,
            batch_gradient=self.batch_gradient,
            cpus=self.cpus,
            no_run_visuals=self.no_run_visuals,
            connect_timeout=self.connect_timeout,
            request_timeout=self.request_timeout,
            graceful_stop=self.graceful_stop,
            reset_connections=self.reset_connections,
            browser_type=self.browser_type,
            device_type=self.device_type,
            locale=self.locale,
            geolocation=self.geolocation,
            permissions=self.permissions,
            playwright_options=self.playwright_options,
            tracing=self.tracing,
            actions_filepaths=self.actions_filepaths
        )

        self.client = Client(
            self.graph_name,
            self.graph_id,
            self.name,
            self.stage_id
        )
        self.client._config = self.config
    
        self.experiment = self.experiment
        if self.experiment:
            self.experiment.source_batch_size = self.batch_size

        self.source_internal_events = [
            'collect_target_stages'
        ]

        self.internal_events = [
            'collect_target_stages',
            'configure_target_stages',
            'collect_action_hooks',
            'check_actions_setup_needed',
            'setup_action',
            'collect_task_hooks',
            'check_tasks_setup_needed',
            'setup_task',
            'apply_channels',
            'complete'
        ]

        self.tracing = self.tracing
        self.priority = self.priority
        if self.priority is None:
            self.priority = 'auto'

        self.priority = self.priority
        if self.priority is None:
            self.priority = 'auto'

        self.priority_level: StagePriority = StagePriority.map(
            self.priority
        )

    @Internal()
    async def run(self):
        await self.setup_events()
        self.dispatcher.assemble_execution_graph()
        await self.dispatcher.dispatch_events(self.name)

    @Internal()
    async def run_internal(self):
        await self.setup_events()
        
        initial_events = dict(**self.dispatcher.initial_events)
        self.dispatcher.skip_list.extend([
            stage_event.event_name for stage_event in self.dispatcher.events_by_name.values() if stage_event.source.shortname not in self.internal_events
        ])

        self.dispatcher.initial_events = initial_events
        self.dispatcher.assemble_execution_graph()
        await self.dispatcher.dispatch_events(self.name)
    
    @context()
    async def collect_target_stages(
        self,
        setup_stage_target_stages: Dict[str, Stage]={},
        setup_stage_target_config: Config=None,
        setup_stage_is_primary_thread: bool = True
    ):
  
        bypass_connection_validation = self.core_config.get('bypass_connection_validation', False)
        connection_validation_retries = self.core_config.get('connection_validation_retries', 3)

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Starting setup')
        
        return {
            'setup_stage_experiment_config': {},
            'setup_stage_target_stages_count': len(setup_stage_target_stages),
            'setup_stage_target_config': setup_stage_target_config,
            'setup_stage_target_stages': setup_stage_target_stages,
            'bypass_connection_validation': bypass_connection_validation,
            'connection_validation_retries': connection_validation_retries,
            'setup_stage_is_primary_thread': setup_stage_is_primary_thread
        }

    @event('collect_target_stages')
    async def configure_target_stages(
        self, 
        setup_stage_target_stages: Dict[str, Stage]={},
        setup_stage_target_config: Config=None,
        setup_stage_is_primary_thread: bool=True,
        setup_stage_experiment_config: Dict[str, Union[str, int, List[float]]]={},
    ):
        execute_stage_names = ', '.join(list(setup_stage_target_stages.keys()))

        setup_stage_configs = {}

        await self.logger.spinner.append_message(f'Setting up - {execute_stage_names}')

        execute_stage_id = 1

        if setup_stage_is_primary_thread and self.experiment:
            self.experiment.assign_weights()
        
        for execute_stage_name, execute_stage in setup_stage_target_stages.items():

            execute_stage.execution_stage_id = execute_stage_id
            execute_stage.execute_setup_stage = self.name

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Execute stage - {execute_stage_name} - assigned stage order id - {execute_stage_id}')

            execute_stage_id += 1

            persona_plugins: Dict[str, PersonaPlugin] = self.plugins_by_type[PluginType.PERSONA]
            for plugin_name, plugin in persona_plugins.items():
                plugin.name = plugin_name
                self.persona_types.types[plugin_name] = plugin
                execute_stage.plugins[plugin_name] = plugin
                
                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Persona plugin - {plugin.name} - for Execute stae - {execute_stage_name}')
   
            engine_plugins: Dict[str, EnginePlugin] = self.plugins_by_type[PluginType.ENGINE]

            for plugin_name, plugin in engine_plugins.items():
                execute_stage.client.plugin[plugin_name] = plugin(setup_stage_target_config)
                plugin.name = plugin_name
                execute_stage.plugins[plugin_name] = plugin
                self.plugins_by_type[plugin_name] = plugin

                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Engine plugin - {plugin.name} - for Execute stage - {execute_stage_name}')

            existing_stage_config = setup_stage_configs.get(execute_stage_name)
            if existing_stage_config:
                return {
                    'setup_stage_configs': setup_stage_configs,
                    'setup_stage_target_stages': setup_stage_target_stages,
                    'setup_stage_experiment_config': setup_stage_experiment_config
                }

            config_copy = setup_stage_target_config.copy()

            if self.tracing:
                config_copy.tracing = self.tracing

            if self.experiment and self.experiment.is_variant(execute_stage_name):


                variant = self.experiment.get_variant(execute_stage_name)

                experiment = {
                    'experiment_name': self.experiment.experiment_name,
                    'random': self.experiment.random,
                    'weight': variant.weight
                }

                if setup_stage_is_primary_thread:
                    config_copy.batch_size = self.experiment.get_variant_batch_size(
                        execute_stage_name
                    )

                    distribution = self.experiment.calculate_distribution(
                        execute_stage_name,
                        config_copy.batch_size
                    )

                    if distribution is not None:


                        experiment.update({
                            'distribution_type': variant.distribution.selected_distribution,
                            'distribution': distribution,
                            'intervals': variant.intervals,
                            'interval_duration': round(
                                config_copy.total_time/(variant.intervals - 1), 
                                2
                            )
                        })

                        config_copy.experiment = experiment
                        config_copy.persona_type = self.persona_types['approx-dist']

                if variant.mutations:
                    config_copy.mutations = variant.get_mutations()

                setup_stage_experiment_config[execute_stage_name] = experiment

            setup_stage_configs[execute_stage_name] = config_copy
            setup_stage_target_stages[execute_stage_name] = execute_stage

        return  {
            'setup_stage_configs': setup_stage_configs,
            'setup_stage_target_stages': setup_stage_target_stages,
            'setup_stage_experiment_config': setup_stage_experiment_config
        }

    @event('configure_target_stages')
    async def collect_action_hooks(
        self,
        setup_stage_target_stages: Dict[str, Stage]={}
    ):
        actions: List[ActionHook] = []
        for execute_stage in setup_stage_target_stages.values():
            actions.extend(execute_stage.hooks[HookType.ACTION])

        return {
            'setup_stage_actions': actions
        }

    
    @condition('collect_action_hooks')
    async def check_actions_setup_needed(
        self, 
        setup_stage_actions: List[ActionHook]=[]
    ):
        
        return {
            'setup_stage_has_actions': len(setup_stage_actions) > 0
        }
        
    @transform('check_actions_setup_needed')
    async def setup_action(
        self,
        setup_stage_configs: Dict[str, Config] = {},
        setup_stage_actions: ActionHook=None,
        setup_stage_has_actions: bool = False,
        bypass_connection_validation: bool=False,
        connection_validation_retries: int=3,
        setup_stage_target_config: Config=None
    ):
            if setup_stage_has_actions and isinstance(setup_stage_actions, ActionHook) and setup_stage_actions.skip is False:

                hook = setup_stage_actions
                hook.skip = setup_stage_actions.skip
                hook.stage_instance.client.next_name = hook.name
                hook.stage_instance.client.intercept = True

                config_copy = setup_stage_configs.get(
                    hook.stage
                )
                
                hook.stage_instance.client._config = config_copy

                execute_stage_name = hook.stage_instance.name

                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Client intercept set to {hook.stage_instance.client.intercept} - Action calls for client id - {hook.stage_instance.client.client_id} - will be suspended on execution')

                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Setting up Action - {hook.name}:{hook.hook_id} - for Execute stage - {execute_stage_name}')
                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Preparing Action hook - {hook.name}:{hook.hook_id} - for suspension - Execute stage - {execute_stage_name}')

                hook.stage_instance.client.actions.set_waiter(hook.stage_instance.name)
                
                setup_call = SetupCall(
                    hook, 
                    config_copy, 
                    retries=connection_validation_retries
                )

                setup_call.metadata_string = self.metadata_string
                setup_call.action_store = hook.stage_instance.client.actions

                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Executing Action hook call - {hook.name}:{hook.hook_id} - Execute stage - {execute_stage_name}')

                task = asyncio.create_task(setup_call.setup())
                await hook.stage_instance.client.actions.wait_for_ready(setup_call)   

                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Exiting suspension for Action - {hook.name}:{hook.hook_id} - Execute stage - {execute_stage_name}')

                action = None
                session = None

                try:
                    if setup_call.exception:
                        raise HookSetupError(
                            hook, 
                            HookType.ACTION, 
                            str(setup_call.exception)
                        )

                    task.cancel()
                    if task.cancelled() is False:
                        await asyncio.wait_for(task, timeout=0.1)

                except HookSetupError as hook_setup_exception:
                    if bypass_connection_validation:

                        action.hook_type = HookType.TASK

                        hook.stage_instance.hooks[HookType.TASK].append(hook)
                        action_idx = hook.stage_instance.hooks[HookType.ACTION].index(hook)
                        hook.stage_instance.hooks[HookType.ACTION].pop(action_idx)

                    else:
                        raise hook_setup_exception

                except asyncio.InvalidStateError:
                    pass

                except asyncio.CancelledError:
                    pass

                except asyncio.TimeoutError:
                    pass
                
                action, session = hook.stage_instance.client.actions.get(
                    hook.stage_instance.name,
                    hook.name
                )

                await session.set_pool(setup_stage_target_config.batch_size)

                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Successfully retrieved prepared Action and Session for action - {hook.name}:{action.action_id} - Execute stage - {execute_stage_name}')
                    
                if len(hook.before) > 0:
                    action.hooks.before = hook.before

                if len(hook.after) > 0:
                    action.hooks.after = hook.after

                if len(hook.checks) > 0:
                    action.hooks.checks = hook.checks
                    
                hook.session = session
                hook.action = action  

                return {
                    'setup_stage_actions': setup_stage_actions
                }

    @event('setup_action')
    async def collect_task_hooks(
        self,
        setup_stage_target_stages: Dict[str, Stage]={}
    ):
        tasks: List[TaskHook] = []
        for execute_stage in setup_stage_target_stages.values():
            tasks.extend(execute_stage.hooks[HookType.TASK])

        return {
            'setup_stage_tasks': tasks
        }
    
    @condition('collect_task_hooks')
    async def check_tasks_setup_needed(
        self, 
        setup_stage_tasks: List[ActionHook]=[]
    ):
        return {
            'setup_stage_has_tasks': len(setup_stage_tasks) > 0
        }

    @transform('check_tasks_setup_needed')
    async def setup_task(
        self,
        setup_stage_tasks: TaskHook=None,
        setup_stage_has_tasks: bool=False,
        setup_stage_configs: Dict[str, Config] = {},
    ):
        if setup_stage_has_tasks and isinstance(setup_stage_tasks, TaskHook) and setup_stage_tasks.skip is False:
            hook = setup_stage_tasks
            execute_stage: Stage = hook.stage_instance
            execute_stage.client.next_name = hook.name
            execute_stage.client.intercept = True

            config_copy = setup_stage_configs.get(
                hook.stage
            )
                
            execute_stage.client._config = config_copy
            execute_stage.client.set_mutations()

            execute_stage_name = hook.stage_instance.name

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Loading Task hook - {hook.name}:{hook.hook_id} - to Execute stage - {execute_stage_name}')

            execute_stage.client.next_name = hook.name
            task, session = execute_stage.client.task.call(
                hook.call,
                env=hook.metadata.env,
                user=hook.metadata.user,
                tags=hook.metadata.tags
            )
            
            await session.set_pool(config_copy.batch_size)

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Successfully retrieved task and session for Task - {hook.name}:{task.action_id} - Execute stage - {execute_stage_name}')

            if len(hook.before) > 0:
                task.hooks.before = hook.before

            if len(hook.after) > 0:
                task.hooks.after = hook.after

            if len(hook.checks) > 0:
                task.hooks.checks = hook.checks

            
            if hook.is_notifier:
                task.hooks.notify = hook.is_notifier
                task.hooks.channels = hook.channels
                task.hooks.listeners = {
                    listener.shortname: listener for listener in hook.listeners
                }

            elif hook.is_listener:
                task.hooks.listen = hook.is_listener

            hook.session = session
            hook.action = task  

            return {
                'setup_stage_tasks': setup_stage_tasks
            }
        

    @context('setup_task')
    async def apply_channels(
        self,
        setup_stage_configs: Dict[str, Config] = {},
        setup_stage_target_stages: Dict[str, Stage]=[],
        setup_stage_actions: List[ActionHook]=[],
        setup_stage_tasks: List[TaskHook]=[],
        setup_stage_target_config: Config=None,
        setup_stage_experiment_config: Dict[str, Union[str, int, List[float]]]={},
    ):
        actions_by_stage = defaultdict(list)
        tasks_by_stage = defaultdict(list)

        for action in setup_stage_actions:
            actions_by_stage[action.stage].append(action)

        for task in setup_stage_tasks:
            tasks_by_stage[task.stage].append(task)

        execute_stage_setup_hooks = defaultdict(list)

        for execute_stage in setup_stage_target_stages.values():

            execute_stage.client.intercept = False
            execute_stage.hooks[HookType.ACTION] = actions_by_stage[execute_stage.name]
            execute_stage.hooks[HookType.TASK] = tasks_by_stage[execute_stage.name]
            execute_stage.context['execute_stage_setup_hooks'] = [
                *actions_by_stage[execute_stage.name],
                *tasks_by_stage[execute_stage.name]
            ]

            execute_stage_setup_hooks[execute_stage.name].extend([
                *actions_by_stage[execute_stage.name],
                *tasks_by_stage[execute_stage.name]
            ])

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Client intercept set to {execute_stage.client.intercept} - Action calls for client id - {execute_stage.client.client_id} - will not be suspended on execution')

            self.stages[execute_stage.name] = execute_stage

            actions_generated_count = len(execute_stage.hooks[HookType.ACTION])
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Generated - {actions_generated_count} - Actions for Execute stage - {execute_stage.name}')

            tasks_generated_count = len(execute_stage.hooks[HookType.TASK])
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Generated - {tasks_generated_count} - Tasks for Execute stage - {execute_stage.name}')

        return {
            'setup_stage_experiment_config': setup_stage_experiment_config,
            'setup_stage_configs': setup_stage_configs,
            'execute_stage_setup_by': self.name,
            'execute_stage_setup_hooks': execute_stage_setup_hooks,
            'execute_stage_setup_config': setup_stage_target_config,
            'setup_stage_ready_stages': setup_stage_target_stages,
            'setup_stage_target_stages': setup_stage_target_stages
        }

    @event('apply_channels')
    async def complete(self):
        return {}

