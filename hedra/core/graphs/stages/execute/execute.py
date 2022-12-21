
import dill
import time
import asyncio
import statistics
from typing import Generic, List, Union
from hedra.core.engines.client import Client
from typing_extensions import TypeVarTuple, Unpack
from hedra.core.graphs.events import Event
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.registry import registered_engines
from hedra.core.graphs.hooks.registry.registry_types import (
    ActionHook,
    EventHook, 
    ContextHook,
    TaskHook
)
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.hook_types.internal import Internal
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.engines.types.playwright import (
    MercuryPlaywrightClient,
    ContextConfig
)
from hedra.core.personas.persona_registry import get_persona, registered_personas
from hedra.plugins.types.plugin_types import PluginType

from hedra.core.graphs.stages.base.parallel.partition_method import PartitionMethod
from hedra.core.graphs.stages.base.stage import Stage
from .parallel import execute_actions


T = TypeVarTuple('T')


class Execute(Stage, Generic[Unpack[T]]):
    stage_type=StageTypes.EXECUTE

    def __init__(self) -> None:
        super().__init__()
        self.persona = None
        self.client: Client[Unpack[T]] = Client(
            self.graph_name,
            self.graph_id,
            self.name,
            self.stage_id
        )
        
        self.accepted_hook_types = [ 
            HookType.SETUP, 
            HookType.BEFORE, 
            HookType.ACTION,
            HookType.TASK,
            HookType.AFTER,
            HookType.TEARDOWN,
            HookType.CHECK,
            HookType.CHANNEL, 
            HookType.EVENT,
            HookType.CONTEXT
        ]

        self.concurrent_pool_aware_stages = 0
        self.execution_stage_id = 0
        self.optimized = False
        self.execute_setup_stage = None
        self.requires_shutdown = True
        self.allow_parallel = True

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

        config = self.client._config
        persona_type_name = config.persona_type.capitalize()

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing - {config.batch_size} - VUs over {self.workers} threads for {config.total_time_string} using - {persona_type_name} - persona')
        await self.logger.spinner.append_message(f'Stage {self.name} executing - {config.batch_size} - VUs over {self.workers} threads for {config.total_time_string} using - {persona_type_name} - persona')

        engine_plugins = self.plugins_by_type.get(PluginType.ENGINE)
        for plugin in engine_plugins.values():
            registered_engines[plugin.name] = plugin
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Engine plugin - {plugin.name}')

        persona_plugins = self.plugins_by_type.get(PluginType.PERSONA)

        for plugin_name, plugin in persona_plugins.items():
            registered_personas[plugin_name] = lambda config: plugin(config)
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Persona plugin - {plugin.name}')

        if self.workers > 1:

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Provisioning execution over - {self.workers} - workers')

            action_hooks: List[ActionHook] = self.hooks[HookType.ACTION]
            hooks = [
                {
                    'timeouts': hook.session.timeouts,
                    'reset_connections': hook.session.pool.reset_connections,
                    'hook_name': hook.name,
                    'hook_shortname': hook.shortname,
                    'hook_type': hook.hook_type,
                    'stage': hook.stage,
                    'weight': hook.metadata.weight,
                    'order': hook.metadata.order,
                    **hook.action.to_serializable()
                } for hook in action_hooks
            ]

            task_hooks: List[TaskHook] = self.hooks[HookType.TASK]
            hooks.extend([
                {
                    'timeouts': hook.session.timeouts,
                    'reset_connections': False,
                    'hook_name': hook.name,
                    'hook_shortname': hook.shortname,
                    'hook_type': hook.hook_type,
                    'stage': hook.stage,
                    'weight': hook.metadata.weight,
                    'order': hook.metadata.order,
                    **hook.action.to_serializable()
                } for hook in task_hooks
            ])

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Starting execution for - {self.workers} workers')             
    
            results_sets = await self.executor.execute_stage_batch(
                execute_actions,
                [
                    dill.dumps({
                        'graph_name': self.graph_name,
                        'graph_id': self.graph_id,
                        'source_stage_name': self.name,
                        'source_stage_id': self.stage_id,
                        'partition_method': PartitionMethod.BATCHES,
                        'workers': self.workers,
                        'worker_id': idx + 1,
                        'config': self.client._config,
                        'hooks': hooks
                    }) for idx in range(self.executor.max_workers)
                ]
            )

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Completed execution for - {self.workers} workers')            
            
            results = []
            elapsed_times = []
            for result_set in results_sets:
                results.extend(result_set.get('results'))
                elapsed_times.append(result_set.get('total_elapsed'))

            total_results = len(results)
            total_elapsed = statistics.median(elapsed_times)

        else:

            start = time.monotonic()

            persona_config = self.client._config
            persona = get_persona(persona_config)
            persona.setup(self.hooks, self.metadata_string)

            action_and_task_hooks: List[Union[ActionHook, TaskHook]] = [
                *self.hooks[HookType.ACTION],
                *self.hooks[HookType.TASK]
            ]

            for hook in action_and_task_hooks:
                if hook.action.type == RequestTypes.PLAYWRIGHT and isinstance(hook.session, MercuryPlaywrightClient):

                    await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Setting up Playwright Session')

                    await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Playwright Session - {hook.session.session_id} - Browser Type: {persona_config.browser_type}')
                    await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Playwright Session - {hook.session.session_id} - Device Type: {persona_config.device_type}')
                    await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Playwright Session - {hook.session.session_id} - Locale: {persona_config.locale}')
                    await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Playwright Session - {hook.session.session_id} - geolocation: {persona_config.geolocation}')
                    await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Playwright Session - {hook.session.session_id} - Permissions: {persona_config.permissions}')
                    await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Playwright Session - {hook.session.session_id} - Color Scheme: {persona_config.color_scheme}')


                    await hook.session.setup(ContextConfig(
                        browser_type=persona_config.browser_type,
                        device_type=persona_config.device_type,
                        locale=persona_config.locale,
                        geolocation=persona_config.geolocation,
                        permissions=persona_config.permissions,
                        color_scheme=persona_config.color_scheme,
                        options=persona_config.playwright_options
                    ))

            results = await persona.execute()

            elapsed = time.monotonic() - start

            await self.logger.filesystem.aio['hedra.core'].info(
                f'{self.metadata_string} - Execution complete - Time (including addtional setup) took: {round(elapsed, 2)} seconds'
            )  

            total_results = len(results)
            total_elapsed = persona.total_elapsed

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


        await self.logger.filesystem.aio['hedra.core'].info( f'{self.metadata_string} - Completed - {total_results} actions at  {round(total_results/total_elapsed)} actions/second over {round(total_elapsed)} seconds')
        await self.logger.spinner.set_default_message(f'Stage - {self.name} completed {total_results} actions at {round(total_results/total_elapsed)} actions/second over {round(total_elapsed)} seconds')

        context_hooks: List[ContextHook] = self.hooks[HookType.CONTEXT]
        context_hooks: List[ContextHook] = self.hooks[HookType.CONTEXT]
        await asyncio.gather(*[
            asyncio.create_task(context_hook.call(self.context)) for context_hook in context_hooks
        ])
        
        return {
            'results': results,
            'total_results': total_results,
            'total_elapsed': total_elapsed
        }