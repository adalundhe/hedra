import json
import dill
import threading
import os
import traceback
import signal
import os
from typing import Any, Dict, List, Union, Tuple
from hedra.core.engines.client.config import Config
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.core.graphs.stages.optimize.optimization import Optimizer
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.setup.setup import Setup
from hedra.core.graphs.events import get_event
from hedra.core.graphs.events.base_event import BaseEvent
from hedra.core.engines.types.registry import registered_engines
from hedra.core.personas.persona_registry import registered_personas
from hedra.plugins.types.plugin_types import PluginType
from hedra.plugins.types.engine.engine_plugin import EnginePlugin
from hedra.plugins.types.persona.persona_plugin import PersonaPlugin
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
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
from hedra.core.personas import get_persona




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
        execute_stage_plugins: Dict[PluginType, List[str]] = optimization_config.get('execute_stage_plugins')
        execute_stage_linked_events: Dict[Tuple[HookType, str], List[Tuple[str, str]]] = optimization_config.get('execute_stage_linked_events')
        optimize_params: List[str] = optimization_config.get('optimize_params')
        optimize_iterations: int = optimization_config.get('optimizer_iterations')
        optimizer_algorithm: str = optimization_config.get('optimizer_algorithm')
        time_limit: int = optimization_config.get('time_limit')
        batch_size: int = optimization_config.get('execute_stage_batch_size')

        metadata_string = f'Graph - {graph_name}:{graph_id} - thread:{thread_id} - process:{process_id} - Stage: {source_stage_name}:{source_stage_id} - '

        discovered = import_stages(graph_path)

        execute_stage: Stage = discovered.get(execute_stage_name)()
        execute_stage.context.update(source_stage_context)
        execute_stage = set_stage_hooks(execute_stage)

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

        setup_stage = Setup()
        setup_stage.logger.spinner.logger.log_level = 'critical'
        setup_stage.plugins_by_type = plugins_by_type
        setup_stage.generation_setup_candidates = 1
        setup_stage.stages[execute_stage.name] = execute_stage
        setup_stage.config = execute_stage_config

        stages = loop.run_until_complete(setup_stage.run())
        
        setup_execute_stage: Execute = stages.get(execute_stage_name)

        setup_execute_stage_hooks = {}
        for hook_type in setup_execute_stage.hooks:
            setup_execute_stage_hooks.update({
                hook.name: hook for hook in setup_execute_stage.hooks[hook_type]
            })

        loaded_stages: Dict[str, Stage] = {
            setup_execute_stage.name: setup_execute_stage
        }
        pipeline_stages = {
            setup_execute_stage.name: setup_execute_stage
        }
        
        for event_target in execute_stage_linked_events:
            target_hook_stage, target_hook_type, target_hook_name = event_target
            for event_source in execute_stage_linked_events[event_target]:
                event_source_stage, event_hook_type, event_hook_name = event_source
                
                if target_hook_stage == setup_execute_stage.name:

                    source_stage: Stage = loaded_stages.get(event_source_stage)
                    if source_stage is None:
                        source_stage = discovered.get(event_source_stage)()
                        source_stage = set_stage_hooks(source_stage)
                        
                        loaded_stages[source_stage.name] = source_stage

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
            context.update({
                context_key: context_value for context_key, context_value in stage.context.items() if context_key not in stage.context.known_keys
            })

        return {
            'stage': execute_stage.name,
            'config': execute_stage_config,
            'params': results,
            'context': context
        }

    except BrokenPipeError:

        try:
            optimizer._event_loop.close()
            loop.close()
        except RuntimeError:
            pass

        return {}
    
    except Exception as e:
        raise e