import asyncio
import time
from hedra.runners.leader_services.proto.leader_pb2 import PollLeaderJobRequest
from operator import countOf
from hedra.core import Executor
from hedra.reporting import Handler
from hedra.runners.leader_services.proto import PipelineStageRequest
from hedra.runners.utils.connect_timeout import connect_or_return_none

from hedra.core.personas.parallel import parse_time
from .worker_pipeline_config import WorkerPipelineConfig


class WorkerPipeline:

    def __init__(self, config, worker_address=None, worker_id=None, timeout=300) -> None:
        logger = Logger()
        self.session_logger = logger.generate_logger()

        self.distributed_config = config.distributed_config
        self.config = WorkerPipelineConfig()
        self.config.distributed_config = config.distributed_config
        self.reporter_config = None
        self.executor = None
        self.handler = None
        self.worker_address = worker_address
        self.worker_id = worker_id
        self.stage_completed = False
        self.leaders = []
        self.aggregate_events = []
        self.status = None
        self.timeout = timeout

    async def setup_pipeline(self, job, reporter_config, leaders):
        await self._parse_job_executor_config(job.get('executor_config'))
        actions = job.get('actions')

        self.timeout = job.get(
            'timeout', 
            parse_time(
                self.config.executor_config.get('total_time', '00:01:00')
            ) * 10
        )

        self.leaders = leaders
        self.reporter_config = reporter_config
        self.config.actions = actions

        self.executor = Executor(self.config)
        self.handler = Handler(self.config)

        self._worker_poll_rate = job.get('poll_rate', 1)

        await self.handler.initialize_reporter()
        self.status = 'created'

    async def run(self, job_id=None):
        try:

            self.status = 'active'

            await self.executor.setup(self.reporter_config)

            for pipeline_stage in self.executor.pipeline:
                await self.executor.pipeline.execute_stage(pipeline_stage)
                await self.mark_stage_completed(pipeline_stage.name, job_id=job_id)
                await self.wait_for_stage_completed(job_id=job_id)
            
            await self.executor.pipeline.get_results()

            completed_actions = self.executor.pipeline.stats.get('completed_actions')
            self.executor.session_logger.info(f'Processing - {completed_actions} - action results.')

            self.aggregate_events = await self.handler.aggregate(
                self.executor.pipeline.results
            )

            await self.mark_stage_completed('done', job_id=job_id)

        except Exception as err:

            self.status = 'failed'

            self.session_logger.error(f'\nEncountered exception running pipeline - {str(err)}\n')
            await self.mark_stage_completed('done', job_id=job_id)

            return self.handler.reporter.fields

        if job_id:
            self.session_logger.info(f'\nJob - {job_id} - has completed on worker - {str(self.worker_id)}\n')

        else:
            self.session_logger.info(f'Pipeline has completed.')

        self.status = 'completed'
        
        return {
            'aggregated': self.aggregate_events
        }

    async def check_for_completion(self, job_id=None):
        
        completions = []
        
        if len(self.leaders) > 0:
            completions, _ = await asyncio.wait([
                self._check_for_completion(
                    leader, 
                    job_id=job_id
                ) for leader in self.leaders
            ], timeout=self.timeout)

            completions = await asyncio.gather(*completions)

        self.stage_completed = countOf(completions, True) == len(self.leaders)

    @connect_or_return_none(wait_buffer=5, timeout_threshold=15)
    async def _check_for_completion(self, leader, job_id=None):
        request = PollLeaderJobRequest(
            host_address=self.worker_address,
            host_id=str(self.worker_id),
            job_id=job_id
        )

        response = await leader.client.PollLeaderJob(request)

        return response.completed

    async def mark_stage_completed(self, stage_name, job_id=None):
        if len(self.leaders) > 0:
            await asyncio.wait([
                self._mark_stage_completed(
                    leader,
                    stage_name,
                    job_id=job_id
                ) for leader in self.leaders
            ], timeout=self.timeout)


    @connect_or_return_none(wait_buffer=5, timeout_threshold=15)
    async def _mark_stage_completed(self, leader, stage_name, job_id=None):
        request = PipelineStageRequest(
            host_address=self.worker_address,
            host_id=str(self.worker_id),
            job_id=job_id,
            stage_completed=stage_name
        )

        return await leader.client.MarkStageCompleted(request)


    async def wait_for_stage_completed(self, job_id=None):
        elapsed = 0
        start = time.time()
        while self.stage_completed is False and elapsed < self.timeout:
            await self.check_for_completion(job_id=job_id)
            await asyncio.sleep(self._worker_poll_rate)
            elapsed = time.time() - start

        self.stage_completed = False

    async def _parse_job_executor_config(self, executor_config):
        executor_config['batch_size'] = int(executor_config.get('batch_size', 1000))
        executor_config['batch_count'] = int(executor_config.get('batch_count', 0))
        executor_config['warmup'] = int(executor_config.get('warmup', 0))
        executor_config['optimize'] = int(executor_config.get('optimize', 0))
        executor_config['batch_time'] = int(executor_config.get('batch_time', 10))
        executor_config['total_time'] = executor_config.get('total_time', '00:01:00')
        executor_config['optimize_iter_duration'] = int(executor_config.get('optimize_iter_duration', 0))
        executor_config['pool_size'] = int(executor_config.get('pool_size', 1))
        executor_config['no_run_visuals'] = True

        self.config.executor_config = executor_config

    async def update_leaders(self, active_leaders, failed_leaders, leaders):
        leaders = [
            leaders[leader] for leader in active_leaders if leader not in failed_leaders
        ]

        self.leaders = leaders
