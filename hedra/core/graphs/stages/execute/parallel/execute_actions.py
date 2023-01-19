import json
import dill
import threading
import os
import time
import traceback
from typing import Dict, Any, List, Union
from hedra.core.engines.client.config import Config
from hedra.core.graphs.hooks.registry.registry_types import ActionHook, TaskHook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.engines.types.playwright import MercuryPlaywrightClient, ContextConfig
from hedra.core.engines.types.registry import RequestTypes
from hedra.core.engines.types.registry import registered_engines
from hedra.core.personas.persona_registry import registered_personas
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.persona.persona_plugin import PersonaPlugin
from hedra.core.graphs.stages.setup.setup import Setup
from hedra.core.graphs.stages.base.import_tools import import_stages, import_plugins
from hedra.core.personas import get_persona
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)

from hedra.core.graphs.stages.base.parallel.partition_method import PartitionMethod
from .action_assembly import ActionAssembler

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

    stages = import_stages(graph_path)
    plugins_by_type = import_plugins(graph_path)
    execute_stage = stages.get(source_stage_name)()

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

    hooks: Dict[HookType, List[Union[ActionHook, TaskHook]]] = {
        HookType.ACTION: [],
        HookType.TASK: [],
    }
    
    actions = {}
    for hook_action in execution_hooks:
        hook_type = hook_action.get('hook_type', HookType.ACTION)

        if hook_type == HookType.ACTION:

            hook = ActionHook(
                hook_action.get('hook_name'),
                hook_action.get('hook_shortname'),
                None
            )

        else:
            hook = TaskHook(
                hook_action.get('hook_name'),
                hook_action.get('hook_shortname'),
                None
            )

        hook.stage = hook_action.get('stage')

        action_type: str = hook_action.get('type')
        action_assembler = ActionAssembler(
            hook,
            hook_action,
            persona,
            persona_config,
            metadata_string
        )

        assembled_hook = await action_assembler.assemble(action_type, execute_stage)
        actions[assembled_hook.name] = assembled_hook.action
        
        await logger.filesystem.aio['hedra.core'].info(
            f'{metadata_string} - Assembled hook - {hook.name}:{hook.hook_id} - using {action_type.capitalize()} Engine'
        )

        hooks[hook_type].append(assembled_hook)
    
    actions_and_tasks = [
        *hooks.get(HookType.ACTION, []),
        *hooks.get(HookType.TASK, [])
    ]

    setup_stage = Setup()
    setup_stage.plugins_by_type = plugins_by_type
    setup_stage.generation_setup_candidates = 1
    setup_stage.stages[execute_stage.name] = execute_stage
    setup_stage.config = source_stage_config

    await setup_stage.run()

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


    persona.setup(hooks, metadata_string)

    await logger.filesystem.aio['hedra.core'].info(f'{metadata_string} - Starting execution')

    results = await persona.execute()

    elapsed = time.monotonic() - start

    await logger.filesystem.aio['hedra.core'].info(f'{metadata_string} - Execution complete - Time (including addtional setup) took: {round(elapsed, 2)} seconds')

    return {
        'results': results,
        'total_results': len(results),
        'total_elapsed': persona.total_elapsed
    }


def execute_actions(parallel_config: str):

    try:
        import asyncio
        import uvloop
        uvloop.install()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        parallel_config: Dict[str, Any] = dill.loads(parallel_config)

        result = loop.run_until_complete(
            start_execution(parallel_config)
        )

        return result

    except Exception as e:
        print(traceback.format_exc())
        raise e