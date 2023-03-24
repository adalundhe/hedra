import json
import dill
import threading
import os
import signal
import pickle
from collections import defaultdict
from typing import Any, Dict, List, Union
from hedra.core.engines.client.config import Config
from hedra.core.graphs.stages.optimize.optimization import Optimizer
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.setup.setup import Setup
from hedra.core.hooks.types.base.event_graph import EventGraph
from hedra.core.engines.types.playwright import MercuryPlaywrightClient, ContextConfig
from hedra.core.engines.types.registry import registered_engines
from hedra.core.personas.persona_registry import registered_personas
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.persona.persona_plugin import PersonaPlugin
from hedra.core.hooks.types.base.hook_type import HookType
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.hooks.types.action.hook import ActionHook
from hedra.core.hooks.types.task.hook import TaskHook
from hedra.core.engines.types.registry import RequestTypes
from hedra.core.graphs.stages.execute import Execute
from hedra.core.graphs.stages.base.import_tools import (
    import_stages, 
    import_plugins,
    set_stage_hooks
)
from hedra.core.graphs.stages.optimize.optimization.algorithms import registered_algorithms
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)


HooksByType = Dict[HookType, Union[List[ActionHook], List[TaskHook]]]


async def setup_action_channels_and_playwright(
    setup_stage: Setup,
    execute_stage: Execute,
    logger: HedraLogger,
    metadata_string: str,
    persona_config: Config
) -> Execute:

    setup_stage.context = SimpleContext()
    setup_stage.generation_setup_candidates = 1
    setup_stage.context['setup_stage_target_config'] = persona_config
    setup_stage.context['setup_stage_target_stages'] = {
        execute_stage.name: execute_stage
    }

    await setup_stage.run_internal()
    
    stages: Dict[str, Stage] =  setup_stage.context['setup_stage_ready_stages']
    setup_execute_stage: Stage = stages.get(execute_stage.name)

    setup_execute_stage_hooks = {}
    for hook_type in setup_execute_stage.hooks:
        setup_execute_stage_hooks.update({
            hook.name: hook for hook in setup_execute_stage.hooks[hook_type]
        })

    actions_and_tasks: List[Union[ActionHook, TaskHook]] = setup_execute_stage.context['execute_stage_setup_hooks']

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


    return setup_execute_stage


def optimize_stage(serialized_config: str):
    import asyncio
    import uvloop
    uvloop.install()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:

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
        logger.filesystem.sync.create_logfile('hedra.core.log')
        logger.filesystem.sync.create_logfile('hedra.optimize.log')

        logger.filesystem.create_filelogger('hedra.core.log')
        logger.filesystem.create_filelogger('hedra.optimize.log')

        thread_id = threading.current_thread().ident
        process_id = os.getpid()

        optimization_config: Dict[str, Union[str, int, Any]] = dill.loads(serialized_config)

        graph_name: str = optimization_config.get('graph_name')
        graph_path: str= optimization_config.get('graph_path')
        graph_id: str = optimization_config.get('graph_id')
        source_stage_name: str = optimization_config.get('source_stage_name')
        source_stage_id: str = optimization_config.get('source_stage_id')
        source_stage_context: Dict[str, Any] = optimization_config.get('source_stage_context')
        execute_stage_name: str = optimization_config.get('execute_stage_name')
        execute_stage_config: Config = optimization_config.get('execute_stage_config')
        execute_setup_stage_name: Config = optimization_config.get('execute_setup_stage_name')
        execute_stage_plugins: Dict[PluginType, List[str]] = optimization_config.get('execute_stage_plugins')
        optimize_params: List[str] = optimization_config.get('optimize_params')
        optimize_iterations: int = optimization_config.get('optimizer_iterations')
        optimizer_algorithm: str = optimization_config.get('optimizer_algorithm')
        time_limit: int = optimization_config.get('time_limit')
        batch_size: int = optimization_config.get('execute_stage_batch_size')

        metadata_string = f'Graph - {graph_name}:{graph_id} - thread:{thread_id} - process:{process_id} - Stage: {source_stage_name}:{source_stage_id} - '
        discovered: Dict[str, Stage] = import_stages(graph_path)
        
        initialized_stages: Dict[str, Stage] = {}
        hooks_by_type = defaultdict(dict)
        hooks_by_name = {}
        hooks_by_shortname = defaultdict(dict)

        generated_stages = {}
        for stage in discovered.values():
            stage.context = SimpleContext()
            
            initialized_stage =  set_stage_hooks(
                stage(),
                generated_stages
            )

            initialized_stages[initialized_stage.name] = initialized_stage

            for hook_type in initialized_stage.hooks:

                for hook in initialized_stage.hooks[hook_type]:
                    hooks_by_type[hook_type][hook.name] = hook
                    hooks_by_name[hook.name] = hook
                    hooks_by_shortname[hook_type][hook.shortname] = hook

        execute_stage: Stage = initialized_stages.get(execute_stage_name)
        execute_stage.context.update(source_stage_context)

        events_graph = EventGraph(hooks_by_type)
        events_graph.hooks_by_name = hooks_by_name
        events_graph.hooks_by_shortname = hooks_by_shortname
        events_graph.hooks_to_events().assemble_graph().apply_graph_to_events()

        for stage in initialized_stages.values():
            stage.dispatcher.assemble_action_and_task_subgraphs()

        execute_stage_config.batch_size = batch_size
        plugins_by_type = import_plugins(graph_path)

        stage_persona_plugins: List[str] = execute_stage_plugins[PluginType.PERSONA]
        persona_plugins: Dict[str, PersonaPlugin] = plugins_by_type[PluginType.PERSONA]

        stage_engine_plugins: List[str] = execute_stage_plugins[PluginType.ENGINE]
        engine_plugins: Dict[str, EnginePlugin] = plugins_by_type[PluginType.ENGINE]
        
        for plugin_name in stage_persona_plugins:
            plugin = persona_plugins.get(plugin_name)
            plugin.name = plugin_name
            registered_personas[plugin_name] = lambda config: plugin(config)
        
        for plugin_name in stage_engine_plugins:
            plugin = engine_plugins.get(plugin_name)
            plugin.name = plugin_name
            registered_engines[plugin_name] = lambda config: plugin(config)

        for plugin_name, plugin in plugins_by_type[PluginType.OPTIMIZER].items():
            registered_algorithms[plugin_name] = plugin
        
        
        setup_stage: Setup = initialized_stages.get(execute_setup_stage_name)

        setup_execute_stage: Execute = loop.run_until_complete(setup_action_channels_and_playwright(
            setup_stage=setup_stage,
            logger=logger,
            metadata_string=metadata_string,
            persona_config=execute_stage_config,
            execute_stage=execute_stage
        ))

        pipeline_stages = {
            setup_execute_stage.name: setup_execute_stage
        }

        logger.filesystem.sync['hedra.optimize'].info(f'{metadata_string} - Setting up Optimization')

        optimizer = Optimizer({
            'graph_name': graph_name,
            'graph_id': graph_id,
            'source_stage_name': source_stage_name,
            'source_stage_id': source_stage_id,
            'params': optimize_params,
            'stage_name': execute_stage_name,
            'stage_config': execute_stage_config,
            'stage_hooks': setup_execute_stage.hooks,
            'iterations': optimize_iterations,
            'algorithm': optimizer_algorithm,
            'time_limit': time_limit
        })

        def handle_loop_stop(signame):
            try:
                optimizer._event_loop.stop()
                optimizer._event_loop.close()

            except BrokenPipeError:
                os._exit(1)
                
            except RuntimeError:
                os._exit(1)

        for signame in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(signame)
            )

            optimizer._event_loop.add_signal_handler(
                getattr(signal, signame),
                lambda signame=signame: handle_loop_stop(signame)
            )

        results = optimizer.optimize()

        execute_stage_config.batch_size = results.get('optimized_batch_size', execute_stage_config.batch_size)
        execute_stage_config.batch_interval = results.get('optimized_batch_interval', execute_stage_config.batch_gradient)
        execute_stage_config.batch_gradient = results.get('optimized_batch_gradient', execute_stage_config.batch_gradient)

        logger.filesystem.sync['hedra.optimize'].info(f'{optimizer.metadata_string} - Optimization complete')

        context = {}
        for stage in pipeline_stages.values():
            
            stage.context.ignore_serialization_filters = [
                'execute_stage_setup_hooks'
            ]

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
        
        return dill.dumps({
            'stage': execute_stage.name,
            'config': execute_stage_config,
            'params': results,
            'context': context
        })

    except BrokenPipeError:

        try:
            optimizer._event_loop.close()
            loop.close()
        except RuntimeError:
            pass

        return {}
    
    except Exception as e:
        raise e