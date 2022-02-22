import time
import asyncio


class Job:

    def __init__(self, job_id, pipeline) -> None:

        self.job_id = job_id
        self.start = None
        self.elapsed = 0
        self.timeout = pipeline.timeout
        
        self.task = None
        self.registered_leaders = []
        self.excluded_leaders = []
        self.pipeline = pipeline
        self.results = None

        self.status = None

    async def run(self):
        self.task = asyncio.create_task(
            self.pipeline.run(job_id=self.job_id)
        )

    async def set_job_status(self, status):
        self.status = status

    async def start_timer(self):
        self.start = time.time()

    async def update_time_elapsed(self):
        self.elapsed = time.time() - self.start

    async def check_if_deadline_exceeded(self):
        return self.elapsed > self.timeout

    async def attach_leader_to_job(self, leader_address):
        self.registered_leaders += [leader_address]
        self.registered_leaders = list(set(self.registered_leaders))

    async def attach_leader_to_excluded(self, leader_address):
        self.excluded_leaders += [leader_address]
        self.excluded_leaders = list(set(self.excluded_leaders))

    async def remove_leader(self, leader_address):
        if leader_address in self.registered_leaders:
            self.registered_leaders.remove(leader_address)

    async def check_if_completed(self):
        return len(self.registered_leaders) < 1


    async def get_task(self):
        if self.results is None:
            self.results = await self.task
        
        return self.results



