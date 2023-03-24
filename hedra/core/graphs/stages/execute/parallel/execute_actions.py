import json
import asyncio
import threading
import os
import time
import signal
import dill
import pickle
import traceback
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from typing import Dict, Any, List, Union
from hedra.core.engines.client.config import Config
from hedra.core.engines.types.playwright import MercuryPlaywrightClient, ContextConfig
from hedra.core.engines.types.registry import RequestTypes
from hedra.core.engines.types.registry import registered_engines
from hedra.core.hooks.types.base.registrar import registrar
from hedra.core.personas import get_persona
from hedra.core.personas.persona_registry import registered_personas
from hedra.core.hooks.types.base.event_graph import EventGraph
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.task.hook import TaskHook
from hedra.core.graphs.stages.base.parallel.partition_method import PartitionMethod
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.base.import_tools import (
    import_stages, 
    import_plugins, 
    set_stage_hooks
)
from hedra.core.graphs.stages.setup.setup import Setup
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)

from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.persona.persona_plugin import PersonaPlugin


async def start_execution(
    metadata_string: str,
    persona_config: Config,
    setup_stage: Setup,
    workers: int,
    source_stage_name: str
):

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
        LoggerTypes.DISTRIBUTED_FILESYSTEM,
        LoggerTypes.SPINNER
    )

    logging_manager.update_log_level(log_level)
    logging_manager.logfiles_directory = logfiles_directory


    logger = HedraLogger()
    logger.initialize()
    await logger.filesystem.aio.create_logfile('hedra.core.log')
    logger.filesystem.create_filelogger('hedra.core.log')

    start = time.monotonic()

    
    persona = get_persona(persona_config)
    persona.workers = workers

    await setup_stage.run_internal()

    stages: Dict[str, Stage] =  setup_stage.context['setup_stage_ready_stages']

    setup_execute_stage: Stage = stages.get(source_stage_name)
    setup_execute_stage.logger = logger

    actions_and_tasks: List[Union[ActionHook, TaskHook]] = setup_stage.context['execute_stage_setup_hooks'].get(source_stage_name)

    execution_hooks_count = len(actions_and_tasks)
    await logger.filesystem.aio['hedra.core'].info(
        f'{metadata_string} - Executing {execution_hooks_count} actions with a batch size of {persona_config.batch_size} for {persona_config.total_time} seconds using Persona - {persona.type.capitalize()}'
    )


    for hook in actions_and_tasks:

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
    
    pipeline_stages = {
        setup_execute_stage.name: setup_execute_stage
    }
                
    persona.setup(setup_execute_stage.hooks, metadata_string)

    await logger.filesystem.aio['hedra.core'].info(f'{metadata_string} - Starting execution')

    results = await persona.execute()

    elapsed = time.monotonic() - start

    await logger.filesystem.aio['hedra.core'].info(f'{metadata_string} - Execution complete - Time (including addtional setup) took: {round(elapsed, 2)} seconds')

    context = {}

    for stage in pipeline_stages.values():

        for key, value in stage.context.as_serializable():    
            try:
                dill.dumps(value)
            
            except ValueError:
                stage.context.ignore_serialization_filters.append(key)
            
            except TypeError:
                stage.context.ignore_serialization_filters.append(key)

            except pickle.PicklingError:
                stage.context.ignore_serialization_filters.append(key)

        serializable_context = stage.context.as_serializable()

        context.update({
            context_key: context_value for context_key, context_value in serializable_context
        })   

    results_dict =  {
        'results': results,
        'total_results': len(results),
        'total_elapsed': persona.total_elapsed,
        'context': context
    }

    return results_dict


def execute_actions(parallel_config: str):
    import asyncio
    import uvloop
    uvloop.install()

    try:
        loop = asyncio.get_event_loop()
    except Exception:

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
        
        graph_name = parallel_config.get('graph_name')
        graph_path: str= parallel_config.get('graph_path') 
        graph_id = parallel_config.get('graph_id')
        source_stage_name = parallel_config.get('source_stage_name')
        source_stage_context: Dict[str, Any] = parallel_config.get('source_stage_context')
        source_stage_plugins = parallel_config.get('source_stage_plugins')
        source_stage_config: Config = parallel_config.get('source_stage_config')
        source_stage_id = parallel_config.get('source_stage_id')
        source_setup_stage_name = parallel_config.get('source_setup_stage_name')
        thread_id = threading.current_thread().ident
        process_id = os.getpid()

        metadata_string = f'Graph - {graph_name}:{graph_id} - thread:{thread_id} - process:{process_id} - Stage: {source_stage_name}:{source_stage_id} - '

        partition_method = parallel_config.get('partition_method')
        workers = parallel_config.get('workers')
        worker_id = parallel_config.get('worker_id')

        discovered: Dict[str, Stage] = import_stages(graph_path)
        plugins_by_type = import_plugins(graph_path)

        initialized_stages = {}
        hooks_by_type = defaultdict(dict)
        hooks_by_name = {}
        hooks_by_shortname = defaultdict(dict)

        generated_hooks = {}
        for stage in discovered.values():
            stage: Stage = stage()
            stage.context = SimpleContext()
            stage.graph_name = graph_name
            stage.graph_path = graph_path
            stage.graph_id = graph_id

            for hook_shortname, hook in registrar.reserved[stage.name].items():
                hook._call = hook._call.__get__(stage, stage.__class__)
                setattr(stage, hook_shortname, hook._call)

            initialized_stage = set_stage_hooks(
                stage, 
                generated_hooks
            )

            for hook_type in initialized_stage.hooks:

                for hook in initialized_stage.hooks[hook_type]:
                    hooks_by_type[hook_type][hook.name] = hook
                    hooks_by_name[hook.name] = hook
                    hooks_by_shortname[hook_type][hook.shortname] = hook

            initialized_stages[initialized_stage.name] = initialized_stage

        execute_stage: Stage = initialized_stages.get(source_stage_name)
        execute_stage.context.update(source_stage_context)

        setup_stage: Setup = initialized_stages.get(source_setup_stage_name)
        setup_stage.context.update(source_stage_context)


        if partition_method == PartitionMethod.BATCHES and source_stage_config.optimized is False:
            if workers == worker_id:
                source_stage_config.batch_size = int(source_stage_config.batch_size/workers) + (source_stage_config.batch_size%workers)
            
            else:
                source_stage_config.batch_size = int(source_stage_config.batch_size/workers)


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

        for hook_type in setup_stage.hooks:
            for hook in setup_stage.hooks[hook_type]:
                hooks_by_type[hook_type][hook.name] = hook

        events_graph = EventGraph(hooks_by_type)
        events_graph.hooks_by_name = hooks_by_name
        events_graph.hooks_by_shortname = hooks_by_shortname
        events_graph.hooks_to_events().assemble_graph().apply_graph_to_events()

        for stage in initialized_stages.values():
            stage.dispatcher.assemble_action_and_task_subgraphs()

        setup_stage.context = SimpleContext()
        setup_stage.config = source_stage_config
        setup_stage.generation_setup_candidates = 1
        setup_stage.context['setup_stage_target_config'] = source_stage_config
        setup_stage.context['setup_stage_target_stages'] = {
            execute_stage.name: execute_stage
        }
  

        result = loop.run_until_complete(
            start_execution(
                metadata_string,
                source_stage_config,
                setup_stage,
                workers,
                source_stage_name
            )
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