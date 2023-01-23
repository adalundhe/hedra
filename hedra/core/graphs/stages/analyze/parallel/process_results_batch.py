

import asyncio
import time
import dill
import json
import threading
import os
import signal
import traceback
from collections import defaultdict
from typing import Any, Dict, List, Tuple
from hedra.core.graphs.hooks.registry.registry_types.hook import Hook
from hedra.core.graphs.events.event import Event, EventHook
from hedra.core.graphs.hooks.hook_types.hook_type import HookType
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.graphs.stages.base.import_tools import (
    import_stages,
    set_stage_hooks
)
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)
from hedra.reporting.events import EventsGroup


async def process_batch(
        stage: Stage,
        stage_name: str, 
        custom_metric_hook_names: List[str],
        results_batch: List[BaseResult],
        metadata_string: str
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

        events =  defaultdict(EventsGroup)

        custom_metric_hooks = []
        for metric_hook_name in custom_metric_hook_names:
            custom_metric_hook = registrar.all.get(metric_hook_name)
            custom_metric_hooks.append(custom_metric_hook)

        await asyncio.gather(*[
            asyncio.create_task(events[stage_result.name].add(
                    stage,
                    stage_result,
                    stage_name
                )) for stage_result in results_batch
        ])
        
        for events_stage_name, events_group in events.items():  

            await logger.filesystem.aio['hedra.reporting'].debug(
                f'{metadata_string} - Events group - {events_group.events_group_id} - created for Stage - {events_stage_name} - Processed - {events_group.total}'
            )

            events_group.calculate_partial_group_stats()

        elapsed = time.monotonic() - start

        await logger.filesystem.aio['hedra.core'].info(f'{metadata_string} - Results aggregation complete - Took: {round(elapsed, 2)} seconds')

        return dill.dumps(events)


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
    analyze_stage_linked_events = config.get('analyze_stage_linked_events', defaultdict(list))

    discovered = import_stages(graph_path)
    stage: Stage = discovered.get(stage_name)()
    setup_analyze_stage: Stage = set_stage_hooks(stage)
    setup_analyze_stage.context.update(source_stage_context)

    loaded_stages: Dict[str, Stage] = {
        setup_analyze_stage.name: setup_analyze_stage
    }
    pipeline_stage = {
        setup_analyze_stage.name: setup_analyze_stage
    }

    for event_target in analyze_stage_linked_events:
        target_hook_stage, target_hook_type, target_hook_name = event_target
        for event_source in analyze_stage_linked_events[event_target]:
            event_source_stage, _, event_hook_name = event_source

            if target_hook_stage == stage_name:

                source_stage: Stage = loaded_stages.get(stage_name)
                source_stage: Stage = loaded_stages.get(event_source_stage)

                if source_stage is None:
                    source_stage = discovered.get(event_source_stage)()
                    source_stage = set_stage_hooks(source_stage)
                    
                    loaded_stages[source_stage.name] = source_stage


                pipeline_stage[source_stage.name] = source_stage

                event_hooks = [hook.name for hook in source_stage.hooks[HookType.EVENT]]
                event_hook_idx = event_hooks.index(event_hook_name)

                event_hook: EventHook = source_stage.hooks[HookType.EVENT][event_hook_idx]

                target_hook_names = [hook.name for hook in setup_analyze_stage.hooks[target_hook_type]]
                target_hook_idx = target_hook_names.index(target_hook_name)

                target_hook: Hook = setup_analyze_stage.hooks[target_hook_type][target_hook_idx]
                target_hook.stage = source_stage_name
                target_hook.stage_instance = setup_analyze_stage
                target_hook.stage_instance.hooks = setup_analyze_stage.hooks

                event = Event(target_hook, event_hook)
                event.target_key = event_hook.key

                if target_hook_idx >= 0 and isinstance(target_hook, Event):
                    if event_hook.pre is True:
                        target_hook.pre_sources[event_hook.name] = event_hook
                        target_hook.stage_instance.hooks[target_hook.hook_type][target_hook_idx] = target_hook

                    else:
                        target_hook.post_sources[event_hook.name] = event_hook
                        target_hook.stage_instance.hooks[target_hook.hook_type][target_hook_idx] = target_hook
                    
                    registrar.all[event.name] = target_hook

                elif target_hook_idx >= 0:
                    target_hook.stage_instance.hooks[target_hook.hook_type][target_hook_idx] = event
                    registrar.all[event.name] = event
                
                target_hook.stage_instance.linked_events[(target_hook.stage, target_hook.hook_type, target_hook.name)].append(
                    (event_hook.stage, event_hook.hook_type, event_hook.name)
                )
                
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
                setup_analyze_stage,
                stage_name,
                custom_metric_hook_names,
                results_batch,
                meetadata_string
            )
        )

        context = {}
        for stage in pipeline_stage.values():
            context.update({
                context_key: context_value for context_key, context_value in stage.context.items() if context_key not in stage.context.known_keys
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
