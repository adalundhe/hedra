

import asyncio
import time
import dill
import json
import threading
import os
import signal 
from collections import defaultdict
from typing import Any, Dict, List
from hedra.core.hooks.types.base.simple_context import SimpleContext
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.hooks.types.base.event_graph import EventGraph
from hedra.core.hooks.types.base.registrar import registrar
from hedra.core.graphs.stages.base.import_tools import (
    import_stages,
    set_stage_hooks
)
from hedra.core.graphs.stages.base.stage import Stage
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)
from hedra.reporting.processed_result.processed_results_group import ProcessedResultsGroup
dill.settings['byref'] = True

async def process_batch(
        stage: Stage=None,
        stage_name: str=None, 
        custom_metric_hook_names: List[str]=[],
        results_batch: List[BaseResult]=[],
        metadata_string: str=None
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
            LoggerTypes.DISTRIBUTED_FILESYSTEM
        )

        logging_manager.update_log_level(log_level)
        logging_manager.logfiles_directory = logfiles_directory


        logger = HedraLogger()
        logger.initialize()
        await logger.filesystem.aio.create_logfile('hedra.core.log')
        await logger.filesystem.aio.create_logfile('hedra.reporting.log')

        logger.filesystem.create_filelogger('hedra.core.log')
        logger.filesystem.create_filelogger('hedra.reporting.log')

        await logger.filesystem.aio['hedra.core'].info(f'{metadata_string} - Initializing results aggregation')

        start = time.monotonic()

        events =  defaultdict(ProcessedResultsGroup)

        custom_metric_hooks = []
        for metric_hook_name in custom_metric_hook_names:
            custom_metric_hook_set = registrar.all.get(metric_hook_name, [])
            
            for custom_metric_hook in custom_metric_hook_set:
                custom_metric_hooks.append(custom_metric_hook)

        for stage_result in results_batch:
            events[stage_result.name].add(
                    stage,
                    stage_result,
                    stage_name
                )

        for events_stage_name, events_group in events.items():  

            await logger.filesystem.aio['hedra.reporting'].debug(
                f'{metadata_string} - Events group - {events_group.events_group_id} - created for Stage - {events_stage_name} - Processed - {events_group.total}'
            )

            events_group.calculate_partial_group_stats()

        elapsed = time.monotonic() - start

        await logger.filesystem.aio['hedra.core'].info(f'{metadata_string} - Results aggregation complete - Took: {round(elapsed, 2)} seconds')

        return events


def process_results_batch(config: Dict[str, Any]):
    import asyncio
    import uvloop
    uvloop.install()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    graph_name = config.get('graph_name')
    graph_path = config.get('graph_path')
    graph_id = config.get('graph_id')
    source_stage_name = config.get('source_stage_name')
    source_stage_context = config.get('source_stage_context')
    source_stage_id = config.get('source_stage_id')

    thread_id = threading.current_thread().ident
    process_id = os.getpid()

    meetadata_string = f'Graph - {graph_name}:{graph_id} - thread:{thread_id} - process:{process_id} - Stage: {source_stage_name}:{source_stage_id} - '

    stage_name = config.get('analyze_stage_name')
    custom_metric_hook_names = config.get('analyze_stage_metric_hooks', [])
    results_batch = config.get('analyze_stage_batched_results', [])

    discovered: Dict[str, Stage] = import_stages(graph_path)
    
    initialized_stages = {}
    hooks_by_type = defaultdict(dict)
    hooks_by_name = {}
    hooks_by_shortname = defaultdict(dict)

    generated_hooks = {}
    for stage in discovered.values():
        stage.context = SimpleContext()
        initialized_stage =  set_stage_hooks(
            stage(), 
            generated_hooks
        )

        initialized_stages[initialized_stage.name] = initialized_stage

        for hook_type in initialized_stage.hooks:

            for hook in initialized_stage.hooks[hook_type]:
                hooks_by_type[hook_type][hook.name] = hook
                hooks_by_name[hook.name] = hook
                hooks_by_shortname[hook_type][hook.shortname] = hook

    setup_analyze_stage: Stage = initialized_stages.get(stage_name)
    setup_analyze_stage.context = SimpleContext()
    setup_analyze_stage.context.update(source_stage_context)

    events_graph = EventGraph(hooks_by_type)
    events_graph.hooks_by_name = hooks_by_name
    events_graph.hooks_by_shortname = hooks_by_shortname
    events_graph.hooks_to_events().assemble_graph().apply_graph_to_events()
    
    pipeline_stage = {
        setup_analyze_stage.name: setup_analyze_stage
    }
                
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def handle_loop_stop(signame):
        try:
            loop.close()
        except RuntimeError:
            os._exit(1)

    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            lambda signame=signame: handle_loop_stop(signame)
        )

    try:

        events = loop.run_until_complete(
            process_batch(
                stage=setup_analyze_stage,
                stage_name=stage_name,
                custom_metric_hook_names=custom_metric_hook_names,
                results_batch=results_batch,
                metadata_string=meetadata_string
            )
        )

        context = {}
        for stage in pipeline_stage.values():                
            serializable_context = stage.context.as_serializable()
            context.update({
                context_key: context_value for context_key, context_value in serializable_context
            })

        return {
            'events': events,
            'context': context
        }

    except BrokenPipeError:

        try:
            loop.close()
        except RuntimeError:
            pass

        return b''

    except Exception as e:
        raise e
