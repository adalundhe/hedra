import asyncio
import psutil
import traceback
import inspect
from collections import defaultdict
from typing_extensions import TypeVarTuple, Unpack
from typing import Dict, Generic, List, Any, Union, Coroutine
from hedra.core.graphs.hooks.hook_types.condition import condition
from hedra.core.graphs.hooks.hook_types.context import context
from hedra.core.graphs.hooks.hook_types.event import event
from hedra.core.graphs.hooks.hook_types.transform import transform
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.hook_types.internal import Internal
from hedra.core.engines.client.client import Client, config_registry
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.base_action import BaseAction
from hedra.core.engines.types.task.task import Task
from hedra.core.graphs.hooks.registry.registry_types import (
    ActionHook,
    AfterHook,
    BeforeHook,
    ChannelHook,
    CheckHook,
    SetupHook,
    TaskHook
)
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
                await self.hook.call()

            except Exception as setup_exception:
                self.exception = setup_exception

                self.error_traceback = str(traceback.format_exc())
                await self.logger.spinner.system.error(f'{self.metadata_string} - Encountered connection validation error - {str(setup_exception)} - {self.hook_name} Hook: {self.hook.name}:{self.hook.hook_id}')
                await self.logger.filesystem.aio['hedra.core'].error(f'{self.metadata_string} - Encountered connection validation error - {str(setup_exception)} - {self.hook_name} Hook: {self.hook.name}:{self.hook.hook_id}')

                if self.current_try >= self.retries:
                    self.action_store.waiter.set_result(None)
                    break

                else:
                    self.current_try += 1



class Setup(Stage, Generic[Unpack[T]]):
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

    
    def __init__(self) -> None:
        super().__init__()
        self.generation_setup_candidates = 0
        self.stages: Dict[str, Stage] = {}
        self.accepted_hook_types = [ 
            HookType.CONDITION,
            HookType.CONTEXT,
            HookType.EVENT, 
            HookType.SETUP,
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
            playwright_options=self.playwright_options
        )

        self.client = Client(
            self.graph_name,
            self.graph_id,
            self.name,
            self.stage_id
        )
        self.client._config = self.config


        self.internal_events = [
            'collect_target_stages'
        ]

    @Internal()
    async def run(self):
        await self.setup_events()
        await self.dispatcher.dispatch_events()

    @Internal()
    async def run_internal(self):
        await self.setup_events()
        
        initial_events = self.dispatcher.initial_events
        self.dispatcher.initial_events = [
            initial_event for initial_event in self.dispatcher.initial_events if initial_event.source.shortname in self.internal_events
        ]

        await self.dispatcher.dispatch_events()
        self.dispatcher.initial_events = initial_events
    
    @context()
    async def collect_target_stages(
        self,
        setup_stages: Dict[str, Stage]={},
        setup_config: Config=None
    ):
        bypass_connection_validation = self.core_config.get('bypass_connection_validation', False)
        connection_validation_retries = self.core_config.get('connection_validation_retries', 3)

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Starting setup')
        
        return {
            'setup_config': setup_config,
            'setup_stages': setup_stages,
            'bypass_connection_validation': bypass_connection_validation,
            'connection_validation_retries': connection_validation_retries
        }

    @event('collect_target_stages')
    async def configure_target_stages(
        self, 
        setup_stages: Dict[str, Stage]={},
        setup_config: Config=None
    ):
    
        execute_stage_names = ', '.join(list(setup_stages.keys()))

        await self.logger.spinner.append_message(f'Setting up - {execute_stage_names}')

        execute_stage_id = 1
        
        for execute_stage_name, execute_stage in setup_stages.items():

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
                execute_stage.client.plugin[plugin_name] = plugin(setup_config)
                plugin.name = plugin_name
                execute_stage.plugins[plugin_name] = plugin
                self.plugins_by_type[plugin_name] = plugin

                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Engine plugin - {plugin.name} - for Execute stage - {execute_stage_name}')

            execute_stage.client._config = setup_config
            setup_stages[execute_stage_name] = execute_stage

        return  {
            'prepared_stages': setup_stages
        }

    @event('configure_target_stages')
    async def collect_action_hooks(
        self,
        prepared_stages: Dict[str, Stage]={}
    ):
    
        actions: List[ActionHook] = []
        for execute_stage in prepared_stages.values():
            actions.extend(execute_stage.hooks[HookType.ACTION])

        return {
            'actions': actions
        }

    
    @condition('collect_action_hooks')
    async def check_actions_setup_needed(self, actions: List[ActionHook]=[]):
        return {
            'has_actions_for_setup': len(actions) > 0
        }
        
    @transform('check_actions_setup_needed')
    async def setup_action(
        self,
        actions: ActionHook=None,
        has_actions_for_setup: bool = False,
        bypass_connection_validation: bool=False,
        connection_validation_retries: int=3,
        setup_config: Config=None
    ):
            if has_actions_for_setup:
                hook = actions
                hook.stage_instance.client.next_name = hook.name
                hook.stage_instance.client.intercept = True
                hook.stage_instance.client._config = setup_config

                execute_stage_name = hook.stage_instance.name

                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Client intercept set to {hook.stage_instance.client.intercept} - Action calls for client id - {hook.stage_instance.client.client_id} - will be suspended on execution')

                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Setting up Action - {hook.name}:{hook.hook_id} - for Execute stage - {execute_stage_name}')
                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Preparing Action hook - {hook.name}:{hook.hook_id} - for suspension - Execute stage - {execute_stage_name}')

                hook.stage_instance.client.actions.set_waiter(hook.stage_instance.name)

                setup_call = SetupCall(hook, setup_config, retries=connection_validation_retries)

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
                        raise HookSetupError(hook, HookType.ACTION, str(setup_call.exception))

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

                await session.set_pool(setup_config.batch_size)

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
                    'actions': actions
                }

    @event('setup_action')
    async def collect_task_hooks(
        self,
        setup_stages: Dict[str, Stage]={}
    ):
        tasks: List[TaskHook] = []
        for execute_stage in setup_stages.values():
            tasks.extend(execute_stage.hooks[HookType.TASK])

        return {
            'tasks': tasks
        }
    
    @condition('collect_task_hooks')
    async def check_tasks_setup_needed(self, tasks: List[ActionHook]=[]):
        return {
            'has_tasks_for_setup': len(tasks) > 0
        }

    @transform('check_tasks_setup_needed')
    async def setup_task(
        self,
        tasks: TaskHook=None,
        has_tasks_for_setup: bool=False,
        setup_config: Config=None
    ):
        if has_tasks_for_setup:
            hook = tasks
            execute_stage: Stage = hook.stage_instance
            execute_stage.client.next_name = hook.name
            execute_stage.client.intercept = True
            execute_stage_name = execute_stage.name

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Loading Task hook - {hook.name}:{hook.hook_id} - to Execute stage - {execute_stage_name}')

            execute_stage.client.next_name = hook.name
            task, session = execute_stage.client.task.call(
                hook.call,
                env=hook.metadata.env,
                user=hook.metadata.user,
                tags=hook.metadata.tags
            )
            
            await session.set_pool(setup_config.batch_size)

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Successfully retrieved task and session for Task - {hook.name}:{task.action_id} - Execute stage - {execute_stage_name}')

            if len(hook.before) > 0:
                task.hooks.before = hook.before

            if len(hook.after) > 0:
                task.hooks.after = hook.after

            if len(hook.checks) > 0:
                task.hooks.checks = hook.checks

            task.hooks.checks = await self.get_checks(execute_stage, hook.shortname)

            hook.session = session
            hook.action = task  

            return {
                'tasks': tasks
            }

    @context('setup_task')
    async def apply_channels(
        self,
        setup_stages: Dict[str, Stage]=[],
        actions: List[ActionHook]=[],
        tasks: List[TaskHook]=[],
        setup_config: Config=None
    ):
        actions_by_stage = defaultdict(list)
        tasks_by_stage = defaultdict(list)

        for action in actions:
            actions_by_stage[action.stage].append(action)

        for task in tasks:
            tasks_by_stage[task.stage].append(task)

        for execute_stage in setup_stages.values():
            await self.get_channels(execute_stage)

            execute_stage.client.intercept = False
            execute_stage.hooks[HookType.ACTION] = actions_by_stage[execute_stage.name]
            execute_stage.hooks[HookType.TASK] = tasks_by_stage[execute_stage.name]
            execute_stage.context['execute_hooks'] = [
                *actions_by_stage[execute_stage.name],
                *tasks_by_stage[execute_stage.name]
            ]

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Client intercept set to {execute_stage.client.intercept} - Action calls for client id - {execute_stage.client.client_id} - will not be suspended on execution')

            self.stages[execute_stage.name] = execute_stage

            actions_generated_count = len(execute_stage.hooks[HookType.ACTION])
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Generated - {actions_generated_count} - Actions for Execute stage - {execute_stage.name}')

            tasks_generated_count = len(execute_stage.hooks[HookType.TASK])
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Generated - {tasks_generated_count} - Tasks for Execute stage - {execute_stage.name}')
    
        return {
            'setup_config': setup_config,
            'ready_stages': setup_stages
        }

    @event('apply_channels')
    async def complete(self):
        return {}

    @Internal()
    async def get_checks(self, execute_stage: Stage, shortname: str) -> List[Coroutine]:

        checks = []
        checks_hooks: List[CheckHook] = execute_stage.hooks[HookType.CHECK]
        for hook in checks_hooks:
            if shortname in hook.names:
                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Adding Check - {hook.name}:{hook.hook_id} - to Action or Task - {shortname} - for Execute stage - {execute_stage.name}')
                
                checks.append(hook)

        return checks

    @Internal()
    async def get_channels(self, execute_stage: Stage) -> None:
        listeners: Dict[str, Union[ActionHook, TaskHook]] = {}
        notifiers: Dict[str, Union[ActionHook, TaskHook]] = {}
        channels: Dict[str, ChannelHook] = {channel.shortname: channel for channel in execute_stage.hooks[HookType.CHANNEL]}

        actions_and_tasks: List[Union[ActionHook, TaskHook]] = execute_stage.hooks[HookType.ACTION]
        actions_and_tasks.extend(execute_stage.hooks[HookType.TASK])

        for hook in actions_and_tasks:
            action: Union[BaseAction, Task] = hook.action
            if hook.is_listener:
                listeners[hook.name] = hook

                for channel_name in hook.listeners:
                    channel: ChannelHook = channels.get(channel_name)
                    action.hooks.channels.append(channel)
                    channel.listeners.append(hook.name)

            if hook.is_notifier:
                notifiers[hook.name] = hook

                for channel_name in hook.notifiers:
                    channel: ChannelHook = channels.get(channel_name)
                    action.hooks.channels.append(channel)
                    channel.notifiers.append(hook.name)

        for channel in channels.values():
            for notifier_name in channel.notifiers:
                notifier: Union[ActionHook, TaskHook] = notifiers.get(notifier_name)
                action: Union[BaseAction, Task] = notifier.action
                
                action.hooks.listeners: List[Hook] = [listeners.get(listener_name) for listener_name in channel.listeners]
