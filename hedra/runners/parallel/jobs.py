import asyncio
import dill
import traceback

from pycli_tools.arguments.bundler import Bundler
from hedra.core import Executor
from hedra.core.graphs.hooks import (
    Execute
)
from hedra.core.graphs.stages.base.stage import Stage


def run_job(config):
    logger = Logger()
    session_logger = logger.generate_logger()

    try:
        
        import asyncio
        
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        loop = asyncio.get_event_loop()

        config = dill.loads(config)

        job_config = config.get('config')

        if job_config.executor_config.get('engine_type') == 'action-set':
            bundler = Bundler(options={
                'class_type': Stage,
                'package_name': job_config.executor_config.get('import_filepath')
            })


            discovered = bundler.discover()
      
            actions = {}

            for action_set in discovered:
                actions[action_set.__name__] = action_set

            job_config.actions = actions.values()

        worker = Executor(job_config)
        results = loop.run_until_complete(_run_job(worker))
        
        return {
            'stats': worker.pipeline.stats,
            'events': results
        }

    except Exception as err:
        session_logger.info(f'{traceback.format_exc()}')
        session_logger.info(f'Parallel pipeline encountered exception - {str(err)}')

async def _run_job(worker):
    await worker.setup()

    for pipeline_stage in worker.pipeline:
        stage.value = pipeline_stage.name
        await worker.pipeline.execute_stage(pipeline_stage)

    stage.value = b'serializing'
    parsed_results = await worker.calculate_results()

    return parsed_results

def create_job(sync_barrier, process_queue, process_stage, optimal_aps, optimal_batch_size, optimal_batch_time):
    global process_barrier
    process_barrier = sync_barrier

    global processed
    processed = process_queue

    global stage
    stage = process_stage

    global optimized_aps
    optimized_aps = optimal_aps

    global optimized_batch_size
    optimized_batch_size = optimal_batch_size

    global optimized_batch_time
    optimized_batch_time = optimal_batch_time
