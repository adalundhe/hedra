import time


class JobPipeline:

    def __init__(self, job_id, leader_address=None, leader_id=None, timeout=600) -> None:
        self.job_id = job_id
        self.reported_completions =  {
            'setup': 0,
            'warmup': 0,
            'optimize': 0,
            'execute': 0,
            'results': 0,
            'done': 0
        }

        self.stages_count = len(self.reported_completions)
        self.workers_count = 0
        self.current_stage = 'setup'
        self.current_stage_completed = False
        self.completed = False
        self.start = None
        self.elapsed = 0
        self.timeout = timeout
        self.workers = []
        self.excluded_workers = []
        self.leader_address = leader_address
        self.leader_id = leader_id

        self.config = None
        self.configs = {}

    async def setup_pipeline(self, new_job, workers, group_on='server'):
        self.config = new_job
        self.workers_count = len(workers)

        if self.workers_count > 0:

            batch_size = self.config.get('batch_size', 1000)

            job_size = int(batch_size / self.workers_count)
            remainder = 0
            job_sizes = {}

            if batch_size % self.workers_count:
                remainder = batch_size % self.workers_count

            for idx, worker in enumerate(workers):
    
                job_sizes[worker] = job_size

                if idx == (self.workers_count - 1):
                    job_sizes[worker] += remainder

            for worker in workers:
                config = dict(self.config)
                config['batch_size'] = job_sizes[worker]
                self.configs[worker] = config

            self.expected_workers = workers

    async def get_job_config(self, worker):
        if worker.worker_addresses in self.worker_configs:
            return self.worker_configs[worker.worker_address]

        return None

    async def start_timer(self):
        self.start = time.time()

    async def update_job_runtime(self):
        self.elapsed = time.time() - self.start

    async def check_if_timeout_exceeded(self):
        return self.elapsed > self.job_timeout

    async def update_completed(self, job_stage_completed):
        self.reported_completions[job_stage_completed.stage_completed] += 1
        stage_completions = self.reported_completions.get(job_stage_completed.stage_completed, 0)
        if stage_completions >= self.workers_count:
            self.current_stage_completed = True

        else:
            self.current_stage_completed = False

    async def check_if_completed(self):
        self.completed = self.reported_completions['done'] >= len(self.workers) and self.reported_completions['done'] > 0
        return self.completed

    async def set_active_workers(self, active_count):
        self.workers_count = active_count

    async def update_pipeline_workers(self, current_workers, failed_workers):
        self.excluded_workers += failed_workers
        self.excluded_workers = list(set(self.excluded_workers))
        self.workers = [worker for worker in current_workers if worker not in self.excluded_workers]

    async def update_pipeline_status(self, updated_pipeline_status):
        new_pipeline = {}

        for stage in updated_pipeline_status:
            current_reported_completions = self.reported_completions[stage]
            updated_reported_completions = int(updated_pipeline_status[stage])

            new_pipeline[stage] = max(current_reported_completions, updated_reported_completions)

        self.reported_completions = new_pipeline

        return new_pipeline


