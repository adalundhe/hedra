
import time
import threading
import os
import signal 
import dill
from collections import defaultdict
from typing import Any, Dict, List
from hedra.logging import HedraLogger
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.core.graphs.stages.base.exceptions.process_killed_error import ProcessKilledError
from hedra.reporting.processed_result.processed_results_group import ProcessedResultsGroup



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

    metadata_string = f'Graph - {graph_name}:{graph_id} - thread:{thread_id} - process:{process_id} - Stage: {source_stage_name}:{source_stage_id} - '

    stage_name = config.get('analyze_stage_name')
    results_batch: List[BaseResult] = config.get('analyze_stage_batched_results', [])

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def handle_loop_stop(signame):
        try:
            loop.close()

        except BrokenPipeError:
            pass

        except RuntimeError:
            pass

    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(
            getattr(signal, signame),
            lambda signame=signame: handle_loop_stop(signame)
        )

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
