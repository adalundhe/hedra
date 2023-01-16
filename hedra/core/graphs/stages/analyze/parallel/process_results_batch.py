

import asyncio
import time
import dill
import json
import threading
import os
from collections import defaultdict
from typing import Any, Dict, List
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.graphs.hooks.registry.registrar import registrar
from hedra.logging import (
    HedraLogger,
    LoggerTypes,
    logging_manager
)
from hedra.reporting.events import EventsGroup


async def process_batch(
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
    graph_id = config.get('graph_id')
    source_stage_name = config.get('source_stage_name')
    source_stage_id = config.get('source_stage_id')

    thread_id = threading.current_thread().ident
    process_id = os.getpid()

    meetadata_string = f'Graph - {graph_name}:{graph_id} - thread:{thread_id} - process:{process_id} - Stage: {source_stage_name}:{source_stage_id} - '

    stage_name = config.get('analyze_stage_name')
    custom_metric_hook_names = config.get('analyze_stage_metric_hooks', [])
    results_batch = config.get('analyze_stage_batched_results', [])

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        return loop.run_until_complete(
            process_batch(
                stage_name,
                custom_metric_hook_names,
                results_batch,
                meetadata_string
            )
        )

    except Exception as e:
        raise e
