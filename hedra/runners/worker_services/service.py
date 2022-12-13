import asyncio

from hedra.runners.leader_services.job_registry import job

from hedra.runners.worker_services.proto.worker_pb2 import JobCompleteResponse
from .leader_registry import NewLeaderQueue
from .proto import (
    WorkerServerServicer,
    WorkerServerUpdateResponse,
    NewJobResponse,
    PollWorkerResponse,
    WorkerHeartbeatResponse,
    LeaderRegisterWorkerResponse
)
from google.protobuf.json_format import (
    MessageToDict
)
from .jobs_registry import JobRegistry


class WorkerService(WorkerServerServicer):

    def __init__(self, config, repoter_config, worker_address, worker_id):
        super().__init__()
        
        logger = Logger()
        self.session_logger = logger.generate_logger()
        self.worker_address = worker_address
        self.worker_id = worker_id

        self.new_leader_queue = NewLeaderQueue()
        self.job_registry = JobRegistry(
            config, 
            repoter_config, 
            self.worker_address, 
            self.worker_id
        )

    async def CreateNewWorkerJob(self, new_job_request, context):
        new_job = MessageToDict(new_job_request).get('jobConfig')
        job_id = new_job_request.job_id

        await self.job_registry.create_job(
            job_id,
            new_job,
            new_job_request.host_address,
            timeout=new_job_request.job_timeout
        )

        await self.job_registry.start_job(job_id)

        response = NewJobResponse(
            host_address=self.worker_address,
            host_id=str(self.worker_id),
            job_id=job_id
        )

        return response

    async def GetRequestsCompleted(self, update_request, context):

        response = WorkerServerUpdateResponse(
            host_address=self.worker_address,
            host_id=str(self.worker_id)
        )

        completed = {}
        async for job in self.job_registry.iter_active():
            if job.pipeline.status == 'active':
                actions_completed, time_elapsed = await job.pipeline.executor.get_completed()

                if time_elapsed < 1:
                    current_rps = 0

                else:
                    current_rps = actions_completed/time_elapsed

                completed[job.job_id] = {
                    'actions_completed': actions_completed,
                    'time_elapsed': time_elapsed,
                    'current_rps': current_rps
                }

        response.completed.update(completed)
        return response

    async def GetJobStatus(self, poll_worker_reqest, context):
        return PollWorkerResponse(
            host_address=self.worker_address,
            host_id=self.worker_id,
            completed=self._active_jobs[poll_worker_reqest.job_id]['task'].done()
        )

    async def GetJobResults(self, results_request, context):
        response = JobCompleteResponse(
            host_address=self.worker_address,
            host_id=str(self.worker_id)
        )

        job = await self.job_registry.get_from_active(results_request.job_id)

        if job and job.pipeline.status == 'completed':
            job_results = await job.get_task()
            response.job_results.update(job_results)
            await job.remove_leader(results_request.host_address)
            await self.job_registry.set_job_complete(job.job_id)

        return response

    async def CheckWorkerHeartbeat(self, request, context):
        return WorkerHeartbeatResponse(
            host_address=self.worker_address,
            host_id=str(self.worker_id),
            status='OK'
        )

    async def LeaderRegisterWorker(self, leader_register_request, context):
        await self.new_leader_queue.add(leader_register_request)

        async for job in self.job_registry.iter_active():
            await job.remove_leader(leader_register_request.host_address)

        return LeaderRegisterWorkerResponse(
            host_address=self.worker_address,
            host_id=str(self.worker_id),
            server_address=self.worker_address
        )

