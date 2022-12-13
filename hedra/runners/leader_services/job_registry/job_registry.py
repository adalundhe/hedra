import asyncio
import time
import uuid
from typing import Union

from hedra.runners.leader_services.worker_registry import WorkerServicesManager
from hedra.runners.leader_services.pipelines import JobPipeline
from hedra.runners.leader_services.electorate import ElectionManager
from .job import Job


class JobRegistry:

    def __init__(self) -> None:

        logger = Logger()
        self.session_logger = logger.generate_logger()


        self.electorate = ElectionManager()

        self.results_task = None
        self.jobs = {}
        self.created = {}
        self.active = {}
        self.completed = {}
        self.failed = {}

        self.monitor = None
        self.ready = False
        self.monitoring = False

        self.leader_ip = None
        self.leader_port = None

        self.workers = WorkerServicesManager()
        
        self.leader_address = None
        self.leader_id = None

        logger = Logger()
        self.session_logger = logger.generate_logger()

        self.heartbeat_interval = None

    async def setup_leader_addresses(self, config):
        await self.electorate.setup_leader_addresses(config)

        self.leader_ip = self.electorate.leader_ip
        self.leader_port = self.electorate.leader_port
        self.leader_address = self.electorate.expected_leaders

    async def setup(self, config, leader_id):
        self.heartbeat_interval = config.distributed_config.get('heartbeat_interval', 1)

        await self.workers.setup(config, leader_id)
        await self.electorate.setup(config, leader_id)

        self.leader_ip = self.electorate.leader_ip
        self.leader_port = int(self.electorate.leader_port)
        self.leader_address = self.electorate.leader_address
        self.workers.leader_ip = self.electorate.leader_ip
        self.workers.leader_port = self.electorate.leader_port
        self.workers.leader_address = self.electorate.leader_address

    async def __iter__(self):
        for job_id in self.jobs:
            yield self.jobs[job_id]

    async def __aiter__(self):
        for job_id in self.jobs:
            yield self.jobs[job_id]

    def __getitem__(self, job_id):
        if job_id in self.jobs:
            return self.jobs[job_id]

        return None

    async def iter_created(self):
        for job_id in self.created:
            yield self.created[job_id]

    async def iter_active(self):
        for job_id in self.active:
            yield self.active[job_id]

    async def iter_completed(self):
        for job_id in self.completed:
            yield self.completed[job_id]

    async def iter_failed(self):
        for job_id in self.completed:
            yield self.completed[job_id]

    async def create_job(self, job_id, new_job, timeout=300):
        if job_id not in self.created and job_id not in self.active:

            self.session_logger.info(f'\nCreating new job id - {job_id}')

            pipeline = JobPipeline(
                job_id,
                leader_address=self.leader_address,
                leader_id=self.leader_id,
                timeout=timeout
            )

            workers = await self.workers.registered()
            await pipeline.setup_pipeline(
                new_job,
                workers
            )

            new_job = Job(job_id, pipeline, new_job)
            for worker in workers:
                await new_job.attach_worker_to_job(worker)

            self.created[job_id] = new_job  
            self.jobs[job_id] = new_job              

            await new_job.set_job_status('created')

    async def start_job(self, job_id):
        if job_id in self.created and job_id not in self.active:
            self.session_logger.info(f'Starting job - {job_id}.')

            job = self.created[job_id]

            await self.workers.create_new_worker_job(job)

            await job.start_timer()
            await job.set_job_status('active')

            self.active[job_id] = job
            
            del self.created[job_id]

    async def update_job_workers(self):
        workers = await self.workers.registered()
        for job_id in self.active:
            active_job = self.active[job_id]
            await active_job.pipeline.update_pipeline_workers(workers, self.workers.registry.failed)

    async def prune_expired_jobs(self):
        active = {}
        
        for job_id in self.active:
            active_job = self.active[job_id]
            await active_job.update_time_elapsed()
            expired = await active_job.check_if_deadline_exceeded()
            
            if expired:
                self.session_logger.info(f'Job - {active_job.job_id} - failed.')
                await active_job.set_job_status('failed')
                self.failed[job_id] = active_job
            else:
                active[job_id] = active_job

        self.active = active

    async def check_for_completed(self):
        active = {}

        for job_id in self.active:
            active_job = self.active[job_id]
            completed =await active_job.pipeline.check_if_completed()

            if completed:
                await active_job.set_job_status('completed')
                job_results = await self.workers.get_worker_job_results(active_job)
                active_job.results_task = asyncio.create_task(self.electorate.elect_leader_and_submit(job_results, job_id=active_job.job_id)) 

                self.completed[job_id] = active_job
            else:
                active[job_id] = active_job

        self.active = active

    async def register_leaders(self, config) -> Union[uuid.UUID, None]:
        return await self.electorate.register_leaders(config)

    async def update_leader_active_jobs(self):
        jobs = dict(self.jobs)
        for job_id in jobs:
            job = jobs[job_id]
            if job.status == 'created' or job.status == 'active':
                updates = await self.electorate.registered_services.share_job(job)

                for updated_pipeline_status in updates:
                    await jobs[job_id].pipeline.update_pipeline_status(updated_pipeline_status)

        self.jobs = jobs

    async def start_job_monitor(self):
        self.monitoring = True
        self.monitor = asyncio.create_task(
            self._monitor_jobs()
        )

    async def _monitor_jobs(self):
        while self.monitoring:
            await self.update_job_workers()
            await self.prune_expired_jobs()
            await asyncio.sleep(self.heartbeat_interval)

            for job_id in self.completed:
                completed_job = self.completed[job_id]
                if completed_job.results_task and completed_job.results_task.done():
                    await completed_job.results_task

            self.failed = {}

    async def stop_job_monitor(self):
        self.monitoring = False
        await self.monitor

    async def create_ephemeral_job(self, job_id, config):
        ephemeral_job = {
            'job_name': job_id,
            'executor_config': config.executor_config,
            'actions': config.actions
        }

        await self.create_job(
            job_id, 
            ephemeral_job, 
            config.distributed_config.get('job_timeout', 600)
        )

    async def wait_for_workers(self):
        start = time.time()
        elapsed = 0
        workers_registered = await self.workers.registry.registered_count_equals_expected_count()
        while workers_registered is False and elapsed < self.workers.worker_request_timeout:
            workers_registered = await self.workers.registry.registered_count_equals_expected_count()
            elapsed = time.time() - start
            await asyncio.sleep(1)

    async def wait_for_leaders(self):
        start = time.time()
        elapsed = 0
        leaders_registered = await self.electorate.registered_services.registered_count_equals_expected_count()
        while leaders_registered is False and elapsed < self.electorate.leader_election_timeout:
            leaders_registered = await self.electorate.registered_services.registered_count_equals_expected_count()
            elapsed = time.time() - start
            await asyncio.sleep(1)
        

