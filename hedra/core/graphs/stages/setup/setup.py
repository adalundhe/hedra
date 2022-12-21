import asyncio
import psutil
import traceback
from typing_extensions import TypeVarTuple, Unpack
from typing import Dict, Generic, List, Any, Union, Coroutine
from hedra.core.graphs.events import Event
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.hook_types.internal import Internal
from hedra.core.engines.client.client import Client
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.base_action import BaseAction
from hedra.core.engines.types.task.task import Task
from hedra.core.graphs.hooks.registry.registry_types import (
    ActionHook,
    AfterHook,
    BeforeHook,
    ChannelHook,
    CheckHook,
    ContextHook,
    EventHook,
    SetupHook,
    TaskHook
)
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.personas.types import PersonaTypesMap
from hedra.logging import HedraLogger
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.plugin_types import PluginType
from playwright.async_api import Geolocation
from hedra.core.graphs.stages.execute import Execute
from hedra.core.graphs.stages.base.stage import Stage
from .exceptions import HookSetupError

T = TypeVarTuple('T')


class SetupCall:

    def __init__(self, hook: Hook, retries: int=1) -> None:
        self.hook = hook
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
        self.stages: Dict[str, Execute] = {}
        self.accepted_hook_types = [ HookType.SETUP, HookType.EVENT, HookType.CONTEXT ]
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

        self.internal_hooks.extend([
            'get_hook',
            'get_checks',
            'setup'
        ])

    @Internal()
    async def run(self):

        events: List[Union[EventHook, Event]] = [event for event in self.hooks[HookType.EVENT]]
        pre_events: List[EventHook] = [
            event for event in events if isinstance(event, EventHook) and event.pre
        ]
        
        if len(pre_events) > 0:
            pre_event_names = ", ".join([
                event.shortname for event in pre_events
            ])

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing PRE events - {pre_event_names}')
            await asyncio.wait([
                asyncio.create_task(event.call()) for event in pre_events
            ], timeout=self.stage_timeout)
        
        bypass_connection_validation = self.core_config.get('bypass_connection_validation', False)
        connection_validation_retries = self.core_config.get('connection_validation_retries', 3)

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Starting setup')

        setup_hooks: List[SetupHook] = self.hooks[HookType.SETUP]
        setup_hook_names = ', '.join([hook.name for hook in setup_hooks])
        
        if len(setup_hooks) > 0:
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Runnning Setup hooks for stage - {setup_hook_names}')
        
        await asyncio.gather(*[hook.call() for hook in setup_hooks])
        execute_stage_id = 1

        stages = dict(self.stages)

        execute_stage_names = ', '.join(list(stages.keys()))

        await self.logger.spinner.append_message(f'Setting up - {execute_stage_names}')
        
        for execute_stage_name, execute_stage in stages.items():

            execute_stage.execution_stage_id = execute_stage_id
            execute_stage.execute_setup_stage = self.name

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Execute stage - {execute_stage_name} - assigned stage order id - {execute_stage_id}')

            execute_stage_id += 1

            persona_plugins = self.plugins_by_type.get(PluginType.PERSONA)
            for plugin_name in persona_plugins.keys():
                self.persona_types.types[plugin_name] = plugin_name
                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Persona plugin - {plugin.name} - for Execute stae - {execute_stage_name}')
   
            client = Client(
                self.graph_name,
                self.graph_id,
                execute_stage.name,
                execute_stage.stage_id
            )
            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Created Client, id - {client.client_id} - for Execute stage - {execute_stage_name}')

            engine_plugins: Dict[str, EnginePlugin] = self.plugins_by_type.get(PluginType.ENGINE)

            for plugin_name, plugin in engine_plugins.items():
                client.plugin[plugin_name] = plugin(self.config)
                plugin.name = plugin_name
                self.plugins_by_type[plugin_name] = plugin

                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Engine plugin - {plugin.name} - for Execute stage - {execute_stage_name}')


            execute_stage.client = client
            execute_stage.client._config = self.config

            action_hooks: List[ActionHook] = execute_stage.hooks[HookType.ACTION]
            for hook in action_hooks:

                execute_stage.client.next_name = hook.name
                execute_stage.client.intercept = True

                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Client intercept set to {execute_stage.client.intercept} - Action calls for client id - {execute_stage.client.client_id} - will be suspended on execution')

                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Setting up Action - {hook.name}:{hook.hook_id} - for Execute stage - {execute_stage_name}')
                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Preparing Action hook - {hook.name}:{hook.hook_id} - for suspension - Execute stage - {execute_stage_name}')

                execute_stage.client.actions.set_waiter(execute_stage.name)

                setup_call = SetupCall(hook, retries=connection_validation_retries)

                setup_call.metadata_string = self.metadata_string
                setup_call.action_store = execute_stage.client.actions

                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Executing Action hook call - {hook.name}:{hook.hook_id} - Execute stage - {execute_stage_name}')

                task = asyncio.create_task(setup_call.setup())
                await execute_stage.client.actions.wait_for_ready(setup_call)   

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

                        hook.hook_type = HookType.TASK

                        execute_stage.hooks[HookType.TASK].append(hook)
                        action_idx = execute_stage.hooks[HookType.ACTION].index(hook)
                        execute_stage.hooks[HookType.ACTION].pop(action_idx)

                    else:
                        raise hook_setup_exception

                except asyncio.InvalidStateError:
                    pass

                except asyncio.CancelledError:
                    pass

                except asyncio.TimeoutError:
                    pass

                if hook.hook_type == HookType.ACTION:

                    action, session = execute_stage.client.actions.get(
                        execute_stage.name,
                        hook.name
                    )

                    await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Successfully retrieved prepared Action and Session for action - {action.name}:{action.action_id} - Execute stage - {execute_stage_name}')
                    
                    action.hooks.before =  await self.get_hook(execute_stage, hook.shortname, HookType.BEFORE)
                    action.hooks.after = await self.get_hook(execute_stage, hook.shortname, HookType.AFTER)
                    action.hooks.checks = await self.get_checks(execute_stage, hook.shortname)
                    
                    hook.session = session
                    hook.action = action    

            task_hooks: List[TaskHook] = execute_stage.hooks[HookType.TASK]
            for hook in task_hooks:

                await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Loading Task hook - {hook.name}:{hook.hook_id} - to Execute stage - {execute_stage_name}')

                execute_stage.client.next_name = hook.name
                task, session = execute_stage.client.task.call(
                    hook.call,
                    env=hook.metadata.env,
                    user=hook.metadata.user,
                    tags=hook.metadata.tags
                )

                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Successfully retrieved task and session for Task - {task.name}:{task.action_id} - Execute stage - {execute_stage_name}')

                task.hooks.checks = await self.get_checks(execute_stage, hook.shortname)

                hook.session = session
                hook.action = task  

            await self.get_channels(execute_stage)

            execute_stage.client.intercept = False
            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Client intercept set to {execute_stage.client.intercept} - Action calls for client id - {execute_stage.client.client_id} - will not be suspended on execution')

            for setup_hook in execute_stage.hooks[HookType.SETUP]:
                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing Setup hook - {setup_hook.name} - for Execute stage - {execute_stage_name}')

                await setup_hook.call()

            self.stages[execute_stage_name] = execute_stage

            actions_generated_count = len(execute_stage.hooks[HookType.ACTION])
            tasks_generated_count = len(execute_stage.hooks[HookType.TASK])

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Generated - {actions_generated_count} - Actions for Execute stage - {execute_stage_name}')
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Generated - {tasks_generated_count} - Tasks for Execute stage - {execute_stage_name}')

        post_events: List[EventHook] = [
            event for event in events if isinstance(event, EventHook) and event.pre is False
        ]

        if len(post_events) > 0:
            post_event_names = ", ".join([
                event.shortname for event in post_events
            ])

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing POST events - {post_event_names}')
            await asyncio.wait([
                asyncio.create_task(event.call()) for event in post_events
            ], timeout=self.stage_timeout)


        await self.logger.spinner.set_default_message(f'Setup for - {execute_stage_names} - complete')
        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Completed setup')

        context_hooks: List[ContextHook] = self.hooks[HookType.CONTEXT]
        context_hooks: List[ContextHook] = self.hooks[HookType.CONTEXT]
        await asyncio.gather(*[
            asyncio.create_task(context_hook.call(self.context)) for context_hook in context_hooks
        ])

        return self.stages

    @Internal()
    async def get_hook(self, execute_stage: Execute, shortname: str, hook_type: str) -> Coroutine:
        hooks: List[Union[ActionHook, BeforeHook]] = execute_stage.hooks[hook_type]
        for hook in hooks:
            if shortname in hook.names:
                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Adding Hook - {hook.name}:{hook.hook_id} - of type - {hook.hook_type.name.capitalize()} - to Action - {shortname} - for Execute stage - {execute_stage.name}')

                return hook.call

    @Internal()
    async def get_checks(self, execute_stage: Execute, shortname: str) -> List[Coroutine]:

        checks = []
        checks_hooks: List[CheckHook] = execute_stage.hooks[HookType.CHECK]
        for hook in checks_hooks:
            if shortname in hook.names:
                await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Adding Check - {hook.name}:{hook.hook_id} - to Action or Task - {shortname} - for Execute stage - {execute_stage.name}')
                
                checks.append(hook.call)

        return checks

    @Internal()
    async def get_channels(self, execute_stage: Execute) -> None:
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
                    action.hooks.channels.append(channel.call)
                    channel.listeners.append(hook.name)

            if hook.is_notifier:
                notifiers[hook.name] = hook

                for channel_name in hook.notifiers:
                    channel: ChannelHook = channels.get(channel_name)
                    action.hooks.channels.append(channel.call)
                    channel.notifiers.append(hook.name)

        for channel in channels.values():
            for notifier_name in channel.notifiers:
                notifier: Union[ActionHook, TaskHook] = notifiers.get(notifier_name)
                action: Union[BaseAction, Task] = notifier.action
                
                action.hooks.listeners: List[Hook] = [listeners.get(listener_name) for listener_name in channel.listeners]

    @Internal()
    async def setup(self) -> None:
        setup_hooks: List[SetupHook] = self.hooks[HookType.SETUP]
        for setup_hook in setup_hooks:
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing Setup hook - {setup_hook.name}')

            await setup_hook()