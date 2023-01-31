
import dill
import time
import statistics
from collections import defaultdict
from typing import Generic, List, Union, Any, Dict
from typing_extensions import TypeVarTuple, Unpack
from hedra.core.engines.client import Client
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.playwright import (
    MercuryPlaywrightClient,
    ContextConfig
)
from hedra.core.engines.types.registry import registered_engines
from hedra.core.graphs.hooks.hook_types.condition import condition
from hedra.core.graphs.hooks.hook_types.context import context
from hedra.core.graphs.hooks.hook_types.event import event
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.hook_types.internal import Internal
from hedra.core.graphs.hooks.registry.registry_types import (
    ActionHook,
    TaskHook
)
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.base.parallel.partition_method import PartitionMethod
from hedra.core.graphs.stages.types.stage_types import StageTypes
from hedra.core.personas.persona_registry import get_persona, registered_personas, DefaultPersona
from hedra.plugins.types.plugin_types import PluginType
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
            HookType.ACTION,
            HookType.AFTER,
            HookType.BEFORE, 
            HookType.CHANNEL, 
            HookType.CHECK,
            HookType.CONDITION,
            HookType.CONTEXT,
            HookType.EVENT,
            HookType.SETUP, 
            HookType.TASK,
            HookType.TEARDOWN,
            HookType.TRANSFORM
        ]

        self.concurrent_pool_aware_stages = 0
        self.execution_stage_id = 0
        self.optimized = False
        self.execute_setup_stage = None
        self.requires_shutdown = True
        self.allow_parallel = True

    @Internal()
    async def run(self):

        await self.setup_events()
        await self.dispatcher.dispatch_events()

    @event()
    async def get_stage_config(self):
        config = self.client._config
        persona_type_name = config.persona_type.capitalize()

        await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Executing - {config.batch_size} - VUs over {self.workers} threads for {config.total_time_string} using - {persona_type_name} - persona')
        await self.logger.spinner.append_message(f'Stage {self.name} executing - {config.batch_size} - VUs over {self.workers} threads for {config.total_time_string} using - {persona_type_name} - persona')

        return {
            'execute_stage_config': config
        }

    @event('get_stage_config')
    async def get_stage_plugins(self):
        engine_plugins = self.plugins_by_type.get(PluginType.ENGINE)
        for plugin in engine_plugins.values():
            registered_engines[plugin.name] = plugin
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Engine plugin - {plugin.name}')

        persona_plugins = self.plugins_by_type.get(PluginType.PERSONA)

        for plugin_name, plugin in persona_plugins.items():
            registered_personas[plugin_name] = lambda plugin_config: plugin(plugin_config)
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Loaded Persona plugin - {plugin.name}')

        source_stage_plugins = defaultdict(list)
        for plugin in self.plugins.values():
            source_stage_plugins[plugin.type].append(plugin.name)

        return {
            'stage_plugins': source_stage_plugins
        }

    @condition('get_stage_plugins')
    async def check_has_multiple_workers(self):
        return {
            'stage_has_multiple_workers': self.workers > 1
        }

    @event('check_has_multiple_workers')
    async def setup_multiple_workers_job(
        self,
        stage_has_multiple_workers: bool = False
    ):
        if stage_has_multiple_workers:

            await self.logger.filesystem.aio['hedra.core'].debug(f'{self.metadata_string} - Provisioning execution over - {self.workers} - workers')

            action_hooks: List[ActionHook] = self.hooks[HookType.ACTION]
            hooks = [
                {
                    'graph_name': self.graph_name,
                    'graph_path': self.graph_path,
                    'stage': hook.stage,
                    'timeouts': hook.session.timeouts,
                    'reset_connections': hook.session.pool.reset_connections,
                    'hook_name': hook.name,
                    'hook_shortname': hook.shortname,
                    'hook_type': hook.hook_type,
                    'stage': hook.stage,
                    'weight': hook.metadata.weight,
                    'order': hook.metadata.order,
                    'checks': [check.__name__ for check in hook.checks],
                    **hook.action.to_serializable()
                } for hook in action_hooks
            ]

            task_hooks: List[TaskHook] = self.hooks[HookType.TASK]

            hooks.extend([
                {
                    'graph_name': self.graph_name,
                    'graph_path': self.graph_path,
                    'stage': hook.stage,
                    'timeouts': hook.session.timeouts,
                    'reset_connections': False,
                    'hook_name': hook.name,
                    'hook_shortname': hook.shortname,
                    'hook_type': hook.hook_type,
                    'stage': hook.stage,
                    'weight': hook.metadata.weight,
                    'order': hook.metadata.order,
                    'checks': [check.__name__ for check in hook.checks],
                    **hook.action.to_serializable()
                } for hook in task_hooks
            ])

            return {
                'stage_hooks': hooks
            }


    @event('setup_multiple_workers_job')
    async def run_multiple_worker_jobs(
        self,
        stage_has_multiple_workers: bool = False,
        execute_stage_config: Config=None,
        stage_hooks: List[Union[ActionHook, TaskHook]]=[],
        stage_plugins: Dict[str, List[Any]]={}
    ):

        if stage_has_multiple_workers:
            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Starting execution for - {self.workers} workers')

            serializable_context = self.context.as_serializable()             

            results_sets = await self.executor.execute_stage_batch(
                execute_actions,
                [
                    dill.dumps({
                        'graph_name': self.graph_name,
                        'graph_path': self.graph_path,
                        'graph_id': self.graph_id,
                        'source_stage_name': self.name,
                        'source_stage_linked_events': self.linked_events,
                        'source_stage_context': {
                            context_key: context_value for context_key, context_value in serializable_context
                        },
                        'source_stage_id': self.stage_id,
                        'source_stage_plugins': stage_plugins,
                        'source_stage_config': execute_stage_config,
                        'partition_method': PartitionMethod.BATCHES,
                        'workers': self.workers,
                        'worker_id': idx + 1,
                        'config': execute_stage_config,
                        'hooks': stage_hooks
                    }) for idx in range(self.executor.max_workers)
                ]
            )

            await self.logger.filesystem.aio['hedra.core'].info(f'{self.metadata_string} - Completed execution for - {self.workers} workers')    


            return {
                'stage_results': results_sets
            }

    @context('run_multiple_worker_jobs')
    async def aggregate_multiple_worker_results(
        self,
        stage_has_multiple_workers: bool = False,
        stage_results: List[List[Any]]=[]
    ):
        if stage_has_multiple_workers:
            aggregate_results = []
            elapsed_times = []
            stage_contexts = defaultdict(list)

            for result_set in stage_results:
                aggregate_results.extend(result_set.get('results'))
                elapsed_times.append(result_set.get('total_elapsed'))

                pipeline_context = result_set.get('context', {})
                for context_key, context_value in pipeline_context.items():
                    stage_contexts[context_key].append(context_value)

            self.context[self.name] = stage_contexts

            total_results = len(aggregate_results)
            total_elapsed = statistics.median(elapsed_times)

            await self.logger.filesystem.aio['hedra.core'].info( f'{self.metadata_string} - Completed - {total_results} actions at  {round(total_results/total_elapsed)} actions/second over {round(total_elapsed)} seconds')
            await self.logger.spinner.set_default_message(f'Stage - {self.name} completed {total_results} actions at {round(total_results/total_elapsed)} actions/second over {round(total_elapsed)} seconds')

            return {
                'stage_results': aggregate_results,
                'total_results': total_results,
                'total_elapsed': total_elapsed
            }

    @event('check_has_multiple_workers')
    async def setup_single_worker_job(
        self,
        stage_has_multiple_workers: bool = False,
        execute_stage_config: Config=None,
    ):

        if stage_has_multiple_workers is False:

            persona_config = execute_stage_config
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
            return {
                'stage_persona': persona
            }


    @context('setup_single_worker_job')
    async def run_single_worker_job(
        self,
        stage_has_multiple_workers: bool = False,
        stage_persona: DefaultPersona=None
    ):
        if stage_has_multiple_workers is False:

            start = time.monotonic()

            results = await stage_persona.execute()

            elapsed = time.monotonic() - start

            await self.logger.filesystem.aio['hedra.core'].info(
                f'{self.metadata_string} - Execution complete - Time (including addtional setup) took: {round(elapsed, 2)} seconds'
            )  

            stage_contexts = defaultdict(list)
            pipeline_context = results.get('context', {})
            for context_key, context_value in pipeline_context.items():
                stage_contexts[context_key].append(context_value)
            
            self.context[self.name] = stage_contexts

            total_results = len(results)
            total_elapsed = stage_persona.total_elapsed

            await self.logger.filesystem.aio['hedra.core'].info( f'{self.metadata_string} - Completed - {total_results} actions at  {round(total_results/total_elapsed)} actions/second over {round(total_elapsed)} seconds')
            await self.logger.spinner.set_default_message(f'Stage - {self.name} completed {total_results} actions at {round(total_results/total_elapsed)} actions/second over {round(total_elapsed)} seconds')

            return {
                'stage_results': results,
                'total_results': total_results,
                'total_elapsed': total_elapsed
            }