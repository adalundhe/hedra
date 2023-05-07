
import time
import threading
import os
import dill
from collections import defaultdict
from typing import Any, Dict, List
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.graphs.stages.base.exceptions.process_killed_error import ProcessKilledError
from hedra.logging import (
    HedraLogger,
    LoggerTypes
)
from hedra.reporting.processed_result.processed_results_group import ProcessedResultsGroup
from hedra.versioning.flags.types.base.active import active_flags
from hedra.versioning.flags.types.base.flag_type import FlagTypes


def process_results_batch(config: Dict[str, Any]):
    
    import warnings
    warnings.simplefilter("ignore")

    from hedra.logging import (
        logging_manager
    )
    
    graph_name = config.get('graph_name')
    graph_id = config.get('graph_id')
    logfiles_directory = config.get('logfiles_directory')
    log_level = config.get('log_level')
    source_stage_name = config.get('source_stage_name')
    source_stage_id = config.get('source_stage_id')
    enable_unstable_features = config.get('enable_unstable_features', False)

    active_flags[FlagTypes.UNSTABLE_FEATURE] = enable_unstable_features

    logging_manager.disable(
        LoggerTypes.DISTRIBUTED,
        LoggerTypes.DISTRIBUTED_FILESYSTEM,
        LoggerTypes.SPINNER
    )

    logging_manager.update_log_level(log_level)
    logging_manager.logfiles_directory = logfiles_directory

    thread_id = threading.current_thread().ident
    process_id = os.getpid()

    metadata_string = f'Graph - {graph_name}:{graph_id} - thread:{thread_id} - process:{process_id} - Stage: {source_stage_name}:{source_stage_id} - '

    stage_name = config.get('analyze_stage_name')
    results_batch: List[BaseResult] = config.get('analyze_stage_batched_results', [])

    try:

        events =  defaultdict(ProcessedResultsGroup)

        logger = HedraLogger()
        logger.initialize()
        logger.filesystem.sync.create_logfile('hedra.core.log')
        logger.filesystem.sync.create_logfile('hedra.reporting.log')
        
        logger.filesystem.sync['hedra.core'].info(f'{metadata_string} - Initializing results aggregation')

        start = time.monotonic()

        for result in results_batch:
            stage_result: BaseResult = dill.loads(result)
            events[stage_result.name].add(
                    stage_name,
                    stage_result,
                )

        for events_stage_name, events_group in events.items():  

            logger.filesystem.sync['hedra.reporting'].debug(
                f'{metadata_string} - Events group - {events_group.events_group_id} - created for Stage - {events_stage_name} - Processed - {events_group.total}'
            )

            events_group.calculate_stats()

        elapsed = time.monotonic() - start

        logger.filesystem.sync['hedra.core'].info(f'{metadata_string} - Results aggregation complete - Took: {round(elapsed, 2)} seconds')

        return events

    except BrokenPipeError:
        raise ProcessKilledError()
    
    except RuntimeError:
        raise ProcessKilledError()

    except Exception as e:
        raise e
