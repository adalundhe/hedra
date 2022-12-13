from .proto import (
    DistributedServerServicer,
    PipelineStageResponse,
    WorkerUpdateResponse,
    WorkerRegisterResponse,
    LeaderRegisterResponse,
    PollLeaderResponse,
    NewJobLeaderResponse,
    LeaderHeartbeatResponse,
    WorkerDeregisterResponse,
    LeaderJobResponse
)
from google.protobuf.json_format import (
    MessageToDict
)

from .worker_registry import NewWorkerQueue
from .job_registry import JobRegistry


class LeaderService(DistributedServerServicer):

    def __init__(self):
        super().__init__()
        
        self.leader_ip = None
        self.leader_port = None
        self.leader_address = None
        self.running = False
        self.new_worker_queue = NewWorkerQueue()
        self.job_registry = JobRegistry()

    async def setup_leader_addresses(self, config):
        await self.job_registry.setup_leader_addresses(config)

        self.leader_ip = self.job_registry.leader_ip
        self.leader_port = self.job_registry.leader_port
        self.leader_address = self.job_registry.leader_address  

    async def setup(self, config, leader_id):
        self.leader_id = leader_id

        await self.job_registry.setup(
            config,
            leader_id
        )      

    async def MarkStageCompleted(self, stage_completed_request, context):

        job = self.job_registry[stage_completed_request.job_id]

        if job:
            await job.pipeline.update_completed(stage_completed_request)

        return PipelineStageResponse(
            host_address=self.job_registry.leader_address,
            host_id=str(self.job_registry.leader_id)
        )

    async def CreateNewJob(self, new_job, context):
        
        config = MessageToDict(new_job.job_config)
        leader_job_id = config.get('job_name')

        if self.job_registry.ready:
            if leader_job_id:
                await self.job_registry.create_job(
                    leader_job_id, 
                    config, 
                    timeout=new_job.job_timeout
                )
                
                await self.job_registry.start_job(leader_job_id)

        return NewJobLeaderResponse(
            host_address=self.job_registry.leader_address,
            host_id=str(self.job_registry.leader_id),
            job_id=leader_job_id
        )

    async def AddClient(self, new_worker_request, context):
        await self.job_registry.workers.registry.register_worker(new_worker_request.server_address)
        
        response = WorkerRegisterResponse(
            host_address=self.job_registry.leader_address,
            host_id=str(self.job_registry.leader_id)
        )

        response.job_config.update({'batch_size': 0, 'batch_time': 0, 'total_time': '00:00:00'})
            
        return response

    async def GetUpdate(self, update_request, context):
        update = await self.job_registry.workers.get_actions_completed()

        response = WorkerUpdateResponse(
            leader_address=self.job_registry.leader_address,
            leader_id=str(self.job_registry.leader_id)
        )
        
        response.completed.update(update)
        return response

    async def RegisterLeader(self, leader_registration_request, context):
        host_config = await self.job_registry.electorate.register(leader_registration_request)     
        return LeaderRegisterResponse(**host_config)

    async def PollLeader(self, request, context):
        return PollLeaderResponse(
            host_address=self.job_registry.leader_address,
            host_id=str(self.job_registry.leader_id),
            completed=self.job_registry.electorate.poll_completed
        )

    async def PollLeaderJob(self, poll_leader_job, context):
        job = self.job_registry[poll_leader_job.job_id]

        completed = False
        if job:
            completed = job.pipeline.current_stage_completed

        return PollLeaderResponse(
            host_address=self.job_registry.leader_address,
            host_id=str(self.job_registry.leader_id),
            completed=completed
        )

    async def CheckLeaderHeartbeat(self, request, context):
        return LeaderHeartbeatResponse(
            host_address=self.job_registry.leader_address,
            host_id=str(self.job_registry.leader_id),
            status='OK'
        )

    async def RemoveClient(self, deregister_request, context):

        self.session_logger.info(f'\nClient from - {deregister_request.host_address} - has requested disconnect.')
        await self.job_registry.stop_job_monitor()

        self.session_logger.debug('Removing worker...')
        await self.job_registry.workers.registry.remove_worker(deregister_request.host_address)

        response = WorkerDeregisterResponse(
            host_address=self.job_registry.leader_address,
            host_id=str(self.job_registry.leader_id)
        )

        self.session_logger.info('Client de-registered.')
        await self.job_registry.start_job_monitor()

        return response

    async def ShareCurrentJob(self, shared_job_request, context):
        job_config = MessageToDict(shared_job_request.job_config)
        job_pipeline_status = MessageToDict(shared_job_request.pipeline_status)

        current_job_config = self.job_registry[shared_job_request.job_id]
        if current_job_config is None:
            await self.job_registry.create_job(shared_job_request.job_id, job_config)
            await self.job_registry.start_job(shared_job_request.job_id)
            updated_pipeline_status = await self.job_registry[shared_job_request.job_id].pipeline.update_pipeline_status(job_pipeline_status)

        else:
            updated_pipeline_status = await self.job_registry[shared_job_request.job_id].pipeline.update_pipeline_status(job_pipeline_status)

        host_job = self.job_registry[shared_job_request.job_id]

        response = LeaderJobResponse(
            host_address=self.job_registry.leader_address,
            host_id=str(self.job_registry.leader_id),
            job_id=shared_job_request.job_id
        )

        response.job_config.update(host_job.config)
        response.pipeline_status.update(updated_pipeline_status)

        return response