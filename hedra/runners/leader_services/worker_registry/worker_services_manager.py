import asyncio
import os
import grpc

from .worker_registry import WorkerRegistry
from hedra.runners.utils.connect_timeout import connect_or_return_none, connect_with_retry_async
from hedra.runners.worker_services.proto import (
    WorkerHeartbeatRequest,
    LeaderRegisterWorkerRequest,
    LeaderUpdateRequest,
    NewJobRequest,
    JobCompleteRequest
)
from google.protobuf.json_format import (
    MessageToDict
)



class WorkerServicesManager:

    def __init__(self) -> None:
        logger = Logger()
        self.session_logger = logger.generate_logger()
        self.leader_ip = None
        self.leader_port = None
        self.leader_id = None
        self.leader_address = None
        self.worker_ips = None
        self.worker_ports = None
        self.worker_request_timeout = None
        self.worker_addresses = None
        self.registry = None

    async def initialize(self):
        await self.registry.initialize()

    async def registered(self):
        return self.registry.worker_addresses

    async def setup(self, config, leader_id):
        self.leader_id = leader_id

        self.worker_ips = config.distributed_config.get(
            'worker_server_ips',
            os.getenv('WORKER_SERVER_IPS')
        )

        self.worker_ports = config.distributed_config.get(
            'worker_server_ports',
            os.getenv('WORKER_SERVER_PORTS', "6671")
        )

        self.worker_request_timeout =config.distributed_config.get(
            'worker_request_timeout',
            30
        )

        worker_addresses = config.distributed_config.get(
            'worker_server_addresses',
            os.getenv('WORKER_SERVER_ADDRESSES')
        )

        workers_count = config.distributed_config.get(
            'workers',
            os.getenv('WORKERS')
        )

        if workers_count:
            self.worker_addresses = []
        
        else:
            if worker_addresses and len(worker_addresses) > 0:
                worker_addresses = worker_addresses.split(",")
                self.worker_addresses = worker_addresses

            elif self.worker_ips and len(self.worker_ips) > 0:
                self.worker_ips = self.worker_ips.split(",")
                self.worker_ports = [int(port) for port in self.worker_ports.split(",")]

                self.worker_addresses = []

                for worker_ip, worker_port in zip(self.worker_ips, self.worker_ports):
                    self.worker_addresses.append(
                        f'{worker_ip}:{worker_port}'
                    )
            
            else:
                self.worker_addresses = []

        worker_address_strings = ''.join([f'\n- {worker_address}' for worker_address in self.worker_addresses])
        
        self.session_logger.info(f'\nConnecting to workers at addresses: {worker_address_strings}\n')

        self.registry = WorkerRegistry(
            self.worker_addresses,
            workers_count=workers_count
        )

    async def add_new_workers(self, new_workers_queue):
        
        has_new_workers = await new_workers_queue.not_empty()
        if has_new_workers:        
            await self.register_leader(workers=new_workers_queue)

        return new_workers_queue

    async def leader_register_worker(self, workers=None):
        if workers is None:
            workers = self.registry

        responses, _ = await asyncio.wait([
            self._leader_register_worker(
                workers[worker_address]
            ) for worker_address in workers.worker_addresses
        ], timeout=self.worker_request_timeout)

        results = await asyncio.gather(*responses)
        
        for registration_response in results:
            if registration_response:
                await self.registry.register_worker(registration_response.host_address)

    @connect_with_retry_async(wait_buffer=5, timeout_threshold=15)
    async def _leader_register_worker(self, worker):
        request =  LeaderRegisterWorkerRequest(
            host_address=self.leader_address,
            host_id=str(self.leader_id)
        )

        return await worker.client.LeaderRegisterWorker(request)

    async def get_actions_completed(self):
        self.session_logger.debug('Getting completed requests...')

        worker_completed_responses = await asyncio.wait([
            self._get_actions_completed(
                self.registry[worker_address],
                worker_address
            ) for worker_address in self.registry.worker_addresses
        ], timeout=self.worker_request_timeout)

        return await asyncio.gather(*worker_completed_responses)

    @connect_with_retry_async(wait_buffer=5,timeout_threshold=15)
    async def _get_actions_completed(self, worker):
        try:
            leader_update_request = LeaderUpdateRequest(
                host_address=self.leader_address,
                host_id=str(self.leader_id)
            )
            update_response = await worker.client.GetRequestsCompleted(leader_update_request)

            return MessageToDict(update_response)

        except Exception as err:
            return {}

    async def create_new_worker_job(self, job):
        worker_job_responses, _ = await asyncio.wait([
            self._create_new_worker_job(
                self.registry[worker_address],
                job,
                worker_address
            ) for worker_address in self.registry.registered_workers
        ], timeout=self.worker_request_timeout)

        return await asyncio.gather(*worker_job_responses)

    @connect_with_retry_async(wait_buffer=5, timeout_threshold=15)
    async def _create_new_worker_job(self, worker, job, worker_address):
        request = NewJobRequest(
                host_address=self.leader_address,
                host_id=str(self.leader_id),
                job_id=job.job_id,
                job_timeout=job.timeout
            )

        request.job_config.update(job.pipeline.configs[worker_address])
        return await worker.client.CreateNewWorkerJob(request)

    async def get_worker_job_results(self, job):
        worker_job_responses, _ = await asyncio.wait([
            self._get_worker_job_results(
                self.registry[worker_address],
                job
            ) for worker_address in self.registry.registered_workers
        ], timeout=self.worker_request_timeout)

        results = await asyncio.gather(*worker_job_responses)
        parsed_results = [MessageToDict(result) for result in results]

        aggregated_events = []
        
        for parsed_result in parsed_results:
            aggregated_events.extend(parsed_result.get('jobResults').get('aggregated'))

        return aggregated_events

    @connect_with_retry_async(wait_buffer=5, timeout_threshold=15)
    async def _get_worker_job_results(self, worker, job):

        request = JobCompleteRequest(
            host_address=self.leader_address,
            host_id=str(self.leader_id),
            job_id=job.job_id
        )

        return await worker.client.GetJobResults(request)

    async def check_worker_heartbeat(self):

        self.session_logger.debug('Getting worker heartbeats...')

        for worker in self.registry:
            worker_heartbeat_response = await self._check_worker_heartbeat(worker)
            await self._update_registry(worker_heartbeat_response, worker.worker_address)
            
    @connect_or_return_none(wait_buffer=1, timeout_threshold=3)
    async def _check_worker_heartbeat(self, worker):
        request = WorkerHeartbeatRequest(
            host_address=self.leader_address,
            host_id=str(self.leader_id),
            status='OK'
        )

        return await worker.client.CheckWorkerHeartbeat(request)

    async def _update_registry(self, worker_response, worker_address):

        if worker_response and isinstance(worker_response, grpc.aio.AioRpcError) is False:
            
            await self.registry.register_worker(worker_response.host_address)

            id_not_set = await self.registry.worker_id_not_set(worker_response.host_address)
            if id_not_set:
                await self.registry.set_worker_id(worker_response.host_address, worker_response.host_id)

        else:
            await self.registry.remove_worker(worker_address)
