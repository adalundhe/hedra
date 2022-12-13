import asyncio
import time
from hedra.runners.leader_services.job_registry import job
from .job import Job

from hedra.runners.worker_services.pipelines import WorkerPipeline
from hedra.runners.worker_services.leader_registry import LeaderServicesManager


class JobRegistry:

    def __init__(self, config, reporter_config, worker_address, worker_id) -> None:
        self.config = config
        self.reporter_config = reporter_config
        self.jobs = {}
        self.created = {}
        self.active = {}
        self.completed = {}
        self.failed = {}

        self.monitor = None
        self.monitoring = False

        worker_ip, worker_port = worker_address.split(':')
        worker_port = int(worker_port)

        self.leaders = LeaderServicesManager(config, worker_ip, worker_port, worker_id)
        
        self.worker_address = worker_address
        self.worker_id = worker_id

        logger = Logger()
        self.session_logger = logger.generate_logger()

        self.heartbeat_interval = config.distributed_config.get('heartbeat_interval', 1)

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

    async def get_from_active(self, job_id):
        if job_id in self.active:
            return self.active[job_id]

        return None

    async def create_job(self, job_id, new_job, leader_address, timeout=300):

        if job_id not in self.created or job_id not in self.active:

            pipeline = WorkerPipeline(
                self.config,
                worker_address=self.worker_address,
                worker_id=self.worker_id,
                timeout=timeout
            )

            leaders = await self.leaders.registered()
            await pipeline.setup_pipeline(
                new_job,
                self.reporter_config,
                leaders
            )

            job = Job(job_id, pipeline)

            await job.attach_leader_to_job(leader_address)
            self.session_logger.info(f'\nAttaching leader - {leader_address} to pending job id - {job_id}\n')


            self.created[job_id] = job
            self.jobs[job_id] = job

            await self.created[job_id].set_job_status('created')

        else:
            job = self.jobs[job_id]

            if leader_address not in job.registered_leaders:
                await job.attach_leader_to_job(leader_address)
                self.session_logger.info(f'Attaching leader - {leader_address} to pending job id - {job_id}\n')

    async def start_job(self, job_id):
        if job_id not in self.active:
            self.session_logger.info(f'Starting job - {job_id}.')

            job = self.created[job_id]

            await job.run()
            await job.start_timer()

            self.active[job_id] = job

            await self.active[job_id].set_job_status('active')

            del self.created[job_id]

        await self.jobs[job_id].set_job_status('active')

    async def update_job_leaders(self):
        active_leaders = await self.leaders.registered()
        for job_id in self.active:
            active_job = self.active[job_id]

            await active_job.pipeline.update_leaders(
                active_leaders,
                self.leaders.registry.failed,
                self.leaders.registry.registered_leaders
            )

    async def prune_expired_jobs(self):
        active = {}

        for job_id in self.active:
            active_job = self.active[job_id]
            await active_job.update_time_elapsed()
            expired = await active_job.check_if_deadline_exceeded()
            
            if expired:
                await active_job.set_job_status('failed')
                self.failed[job_id] = active_job
                self.jobs[job_id] = active_job

            else:
                active[job_id] = active_job
    
        self.active = active

    async def start_job_monitor(self):
        self.monitoring = True
        self.monitor = asyncio.create_task(
            self._monitor_jobs()
        )

    async def _monitor_jobs(self):
        while self.monitoring:
            await self.prune_expired_jobs()
            await asyncio.sleep(self.heartbeat_interval)
            self.failed = {}

    async def stop_job_monitor(self):
        self.monitoring = False
        await self.monitor

    async def set_job_complete(self, job_id):
        active = dict(self.active)
        not_completed = {}

        job = active[job_id]

        job_completed = await job.check_if_completed()
        if job_completed:
            await job.set_job_status('completed')
            self.completed[job_id] = job
            await self.jobs[job_id].set_job_status('completed')

        else:
            not_completed[job_id] = job

        self.active = not_completed

    async def wait_for_leaders(self):
        start = time.time()
        elapsed = 0
        leaders_registered = await self.leaders.registry.registered_count_equals_expected_count()
        while leaders_registered is False and elapsed < self.leaders.leader_request_timeout:
            leaders_registered = await self.leaders.registry.registered_count_equals_expected_count()
            elapsed = time.time() - start
            await asyncio.sleep(1)
