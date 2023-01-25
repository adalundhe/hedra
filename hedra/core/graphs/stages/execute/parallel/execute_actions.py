import json
import threading
import os
import time
import signal
import dill
from hedra.core.graphs.hooks.registry.registrar import registrar
from typing import Dict, Any, List, Union, Tuple
from hedra.core.engines.client.config import Config
from hedra.core.graphs.hooks.registry.registry_types import ActionHook, TaskHook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.engines.types.playwright import MercuryPlaywrightClient, ContextConfig
from hedra.core.engines.types.registry import RequestTypes
from hedra.core.engines.types.registry import registered_engines
from hedra.core.personas.persona_registry import registered_personas
from hedra.core.graphs.events import get_event
from hedra.core.graphs.events.base_event import BaseEvent
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.persona.persona_plugin import PersonaPlugin
from hedra.core.graphs.stages.setup.setup import Setup
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.base.import_tools import (
    import_stages, 
    import_plugins, 
    set_stage_hooks
)
from hedra.core.personas import get_persona
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)

from hedra.core.graphs.stages.base.parallel.partition_method import PartitionMethod


async def start_execution(parallel_config: Dict[str, Any]):

    hedra_config_filepath = os.path.join(
        os.getcwd(),
        '.hedra.json'
    )

    hedra_config = {}
    if os.path.exists(hedra_config_filepath):
        with open(hedra_config_filepath, 'r') as hedra_config_file:
            hedra_config = json.load(hedra_config_file)

    logging_config = hedra_config.get('logging', {})
    logfiles_directory = logging_config.get(
        'logfiles_directory',
        os.getcwd()
    )

    log_level = logging_config.get('log_level', 'info')

    logging_manager.disable(
        LoggerTypes.DISTRIBUTED,
        LoggerTypes.DISTRIBUTED_FILESYSTEM
    )

    logging_manager.update_log_level(log_level)
    logging_manager.logfiles_directory = logfiles_directory


    logger = HedraLogger()
    logger.initialize()
    await logger.filesystem.aio.create_logfile('hedra.core.log')
    logger.filesystem.create_filelogger('hedra.core.log')

    start = time.monotonic()

    graph_name = parallel_config.get('graph_name')
    graph_path: str= parallel_config.get('graph_path') 
    graph_id = parallel_config.get('graph_id')
    source_stage_name = parallel_config.get('source_stage_name')
    source_stage_context: Dict[str, Any] = parallel_config.get('source_stage_context')
    source_stage_linked_events: Dict[Tuple[HookType, str], List[Tuple[str, str]]] = parallel_config.get('source_stage_linked_events')
    source_stage_plugins = parallel_config.get('source_stage_plugins')
    source_stage_config = parallel_config.get('source_stage_config')
    source_stage_id = parallel_config.get('source_stage_id')
    thread_id = threading.current_thread().ident
    process_id = os.getpid()

    metadata_string = f'Graph - {graph_name}:{graph_id} - thread:{thread_id} - process:{process_id} - Stage: {source_stage_name}:{source_stage_id} - '

    execution_hooks = parallel_config.get('hooks')
    partition_method = parallel_config.get('partition_method')
    persona_config: Config = parallel_config.get('config')
    workers = parallel_config.get('workers')
    worker_id = parallel_config.get('worker_id')

    discovered: Dict[str, Stage] = import_stages(graph_path)
    plugins_by_type = import_plugins(graph_path)
    execute_stage: Stage = discovered.get(source_stage_name)()
    execute_stage: Stage = set_stage_hooks(execute_stage)

    execute_stage.context.update(source_stage_context)

    execution_hooks_count = len(execution_hooks)

    if partition_method == PartitionMethod.BATCHES and persona_config.optimized is False:
        if workers == worker_id:
            persona_config.batch_size = int(persona_config.batch_size/workers) + (persona_config.batch_size%workers)
        
        else:
            persona_config.batch_size = int(persona_config.batch_size/workers)


    stage_persona_plugins: List[str] = source_stage_plugins[PluginType.PERSONA]
    persona_plugins: Dict[str, PersonaPlugin] = plugins_by_type[PluginType.PERSONA]

    stage_engine_plugins: List[str] = source_stage_plugins[PluginType.ENGINE]
    engine_plugins: Dict[str, EnginePlugin] = plugins_by_type[PluginType.ENGINE]
    
    for plugin_name in stage_persona_plugins:
        plugin = persona_plugins.get(plugin_name)
        plugin.name = plugin_name
        registered_personas[plugin_name] = lambda config: plugin(config)
    
    for plugin_name in stage_engine_plugins:
        plugin = engine_plugins.get(plugin_name)
        plugin.name = plugin_name
        registered_engines[plugin_name] = lambda config: plugin(config)

    persona = get_persona(persona_config)
    persona.workers = workers


    await logger.filesystem.aio['hedra.core'].info(
        f'{metadata_string} - Executing {execution_hooks_count} actions with a batch size of {persona_config.batch_size} for {persona_config.total_time} seconds using Persona - {persona.type.capitalize()}'
    )

    setup_stage = Setup()
    setup_stage.plugins_by_type = plugins_by_type
    setup_stage.generation_setup_candidates = 1
    setup_stage.stages[execute_stage.name] = execute_stage
    setup_stage.config = source_stage_config

    stages: Dict[str, Stage] = await setup_stage.run()
    setup_execute_stage: Stage = stages.get(source_stage_name)

    actions = {
        hook.name: hook for hook in setup_execute_stage.hooks[HookType.ACTION]
    }

    actions.update({
        hook.name: hook for hook in setup_execute_stage.hooks[HookType.TASK]
    })

    actions_and_tasks: List[Union[ActionHook, TaskHook]] = [
        *setup_execute_stage.hooks.get(HookType.ACTION, []),
        *setup_execute_stage.hooks.get(HookType.TASK, [])
    ]


    for hook in actions_and_tasks:
        if hook.action.hooks.notify:
            for idx, listener_name in enumerate(hook.action.hooks.listeners):
                hook.action.hooks.listeners[idx] = actions.get(listener_name)


        if hook.action.type == RequestTypes.PLAYWRIGHT and isinstance(hook.session, MercuryPlaywrightClient):

            await logger.filesystem.aio['hedra.core'].info(f'{metadata_string} - Setting up Playwright Session')

            await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - Playwright Session - {hook.session.session_id} - Browser Type: {persona_config.browser_type}')
            await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - Playwright Session - {hook.session.session_id} - Device Type: {persona_config.device_type}')
            await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - Playwright Session - {hook.session.session_id} - Locale: {persona_config.locale}')
            await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - Playwright Session - {hook.session.session_id} - geolocation: {persona_config.geolocation}')
            await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - Playwright Session - {hook.session.session_id} - Permissions: {persona_config.permissions}')
            await logger.filesystem.aio['hedra.core'].debug(f'{metadata_string} - Playwright Session - {hook.session.session_id} - Color Scheme: {persona_config.color_scheme}')

            await hook.session.setup(ContextConfig(
                browser_type=persona_config.browser_type,
                device_type=persona_config.device_type,
                locale=persona_config.locale,
                geolocation=persona_config.geolocation,
                permissions=persona_config.permissions,
                color_scheme=persona_config.color_scheme,
                options=persona_config.playwright_options
            ))
    
    loaded_stages: Dict[str, Stage] = {
        setup_execute_stage.name: setup_execute_stage
    }
    pipeline_stages = {
        setup_execute_stage.name: setup_execute_stage
    }

    for event_target in source_stage_linked_events:
        target_hook_stage, target_hook_type, target_hook_name = event_target
        for event_source in source_stage_linked_events[event_target]:
            event_source_stage, _, event_hook_name = event_source
            
            if target_hook_stage == source_stage_name:

                source_stage: Stage = loaded_stages.get(event_source_stage)

                if source_stage is None:
                    source_stage = discovered.get(event_source_stage)()
                    source_stage = set_stage_hooks(source_stage)
                    
                    loaded_stages[source_stage.name] = source_stage


                source_stage = set_stage_hooks(source_stage)

                pipeline_stages[source_stage.name] = source_stage

                source_events = [
                    *source_stage.hooks[HookType.EVENT],
                    *source_stage.hooks[HookType.TRANSFORM]
                ]

                source_event_hook_names = [hook.name for hook in source_events]
                source_hook_idx = source_event_hook_names.index(event_hook_name)

                source_hook: Hook = source_events[source_hook_idx]

                target_hook_names = [hook.name for hook in setup_execute_stage.hooks[target_hook_type]]
                target_hook_idx = target_hook_names.index(target_hook_name)

                target_hook: Hook = setup_execute_stage.hooks[target_hook_type][target_hook_idx]
                target_hook.stage = source_stage_name
                target_hook.stage_instance = setup_execute_stage
                target_hook.stage_instance.hooks = setup_execute_stage.hooks

                event = get_event(target_hook, source_hook)

                if target_hook_idx >= 0 and isinstance(target_hook, BaseEvent):
                    if source_hook.pre is True:
                        target_hook.pre_sources[source_hook.name] = source_hook
                        target_hook.stage_instance.hooks[target_hook.hook_type][target_hook_idx] = target_hook

                    else:
                        target_hook.post_sources[source_hook.name] = source_hook
                        target_hook.stage_instance.hooks[target_hook.hook_type][target_hook_idx] = target_hook
                    
                    registrar.all[event.name] = target_hook

                elif target_hook_idx >= 0:
                    target_hook.stage_instance.hooks[target_hook.hook_type][target_hook_idx] = event
                    registrar.all[event.name] = event
                
                target_hook.stage_instance.linked_events[(target_hook.stage, target_hook.hook_type, target_hook.name)].append(
                    (source_hook.stage, source_hook.hook_type, source_hook.name)
                )

                setup_execute_stage.hooks[target_hook_type][target_hook_idx] = event
                
    persona.setup(setup_execute_stage.hooks, metadata_string)

    await logger.filesystem.aio['hedra.core'].info(f'{metadata_string} - Starting execution')

    results = await persona.execute()

    elapsed = time.monotonic() - start

    await logger.filesystem.aio['hedra.core'].info(f'{metadata_string} - Execution complete - Time (including addtional setup) took: {round(elapsed, 2)} seconds')

    for result in results:
        result.checks = [check.name for check in result.checks]

    context = {}
    for stage in pipeline_stages.values():
        context.update({
            context_key: context_value for context_key, context_value in stage.context.items() if context_key not in stage.context.known_keys
        })   
    
    return {
        'results': results,
        'total_results': len(results),
        'total_elapsed': persona.total_elapsed,
        'context': context
    }


def execute_actions(parallel_config: str):
    import asyncio
    import uvloop
    uvloop.install()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def handle_loop_stop(signame):
        try:
            loop.close()

        except BrokenPipeError:
            os._exit(1)
                
        except RuntimeError:
            os._exit(1)

    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            lambda signame=signame: handle_loop_stop(signame)
        )

    try:
        
        parallel_config: Dict[str, Any] = dill.loads(parallel_config)

        result = loop.run_until_complete(
            start_execution(parallel_config)
        )

        return result

    except BrokenPipeError:
        
        try:
            loop.close()
        except RuntimeError:
            pass

        return {}

    except Exception as e:
        raise e