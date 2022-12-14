from hedra.tools.data_structures.async_list import AsyncList


class JobQueue:

    def __init__(self, batch_size, job_workers) -> None:
        self.batch_size = batch_size
        self.workers = job_workers
        self.jobs_count = job_workers.count
        self.job_sizes = []
        self._job_sizes = AsyncList()
        self.calculated = False

    async def setup_jobs(self):
        job_size = int(self.batch_size / self.jobs_count)
        remainder = 0
        self.job_sizes = []

        if self.batch_size % self.jobs_count:
            remainder = self.batch_size % self.jobs_count

        for _ in range(self.jobs_count):
            self.job_sizes += [job_size]

        self.job_sizes[self.jobs_count - 1] += remainder

        self._job_sizes.data = self.job_sizes
        self.calculated

        return self.job_sizes

    async def map_jobs(self, group_on='address'):
        job_size = int(self.batch_size / self.jobs_count)
        remainder = 0
        self.job_sizes = {}

        if self.batch_size % self.jobs_count:
            remainder = self.batch_size % self.jobs_count

        for idx, worker in enumerate(self.workers):

            if group_on == 'server':
                key = worker.__getattribute__('server')
            
            else:
                key = worker.__getattribute__('address')
            
            self.job_sizes[key] = job_size

            if idx == (self.jobs_count - 1):
                self.job_sizes[key] += remainder

        return self.job_sizes

    async def not_empty(self):
        return await self._job_sizes.size() > 0

    async def get_job(self):
        return await self._job_sizes.pop()