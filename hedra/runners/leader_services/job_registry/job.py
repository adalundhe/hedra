import time

from hedra.core.personas.parallel.time import parse_time

import time
import asyncio


class Job:

    def __init__(self, job_id, pipeline, config) -> None:

        self.config = config
        self.job_id = job_id
        self.start = None
        self.elapsed = 0
        self.timeout = pipeline.timeout
        
        self.task = None
        self.results_task = None
        self.registered_workers = []
        self.excluded_workers = []
        self.pipeline = pipeline
        self.results = None

        self.status = None

    async def set_job_status(self, status):
        self.status = status

    async def start_timer(self):
        self.start = time.time()

    async def update_time_elapsed(self):
        self.elapsed = time.time() - self.start

    async def check_if_deadline_exceeded(self):
        return self.elapsed > self.timeout

    async def attach_worker_to_job(self, leader_address):
        self.registered_workers += [leader_address]
        self.registered_workers = list(set(self.registered_workers))

    async def attach_worker_to_excluded(self, leader_address):
        self.excluded_workers += [leader_address]
        self.excluded_workers = list(set(self.excluded_workers))

    async def remove_leader(self, leader_address):
        if leader_address in self.registered_workers:
            self.registered_workers.remove(leader_address)

    async def get_task(self):
        if self.results is None:
            self.results = await self.task
        
        return self.results



