import asyncio
from asyncio.tasks import wait
import os
import grpc

from hedra.runners.worker_services.bootstrap import BootstrapManager
from .leader_registry import LeaderRegistry

from hedra.runners.leader_services.proto import (
    WorkerRegisterRequest,
    WorkerDeregisterRequest,
    LeaderHeartbeatRequest
)
from hedra.runners.utils.connect_timeout import (
    connect_with_retry_async,
    connect_or_return_none
)


class LeaderServicesManager:

    def __init__(self, config, worker_ip, worker_port, worker_id) -> None:
        logger = Logger()
        self.session_logger = logger.generate_logger()
        self.worker_ip = worker_ip
        self.worker_port = worker_port
        self.worker_id = worker_id
        self.worker_address = f'{self.worker_ip}:{self.worker_port}'

        self.worker_server_port = config.distributed_config.get(
            'worker_server_port',
            os.getenv('WORKER_SERVER_PORT',  "6671")
        )

        self.worker_server_address = f'{self.worker_ip}:{self.worker_server_port}'


        self.leader_ips = config.distributed_config.get(
            'leader_ips',
            os.getenv('LEADER_IPS', 'localhost')
        )

        self.leader_ports = config.distributed_config.get(
            'leader_ports',
            os.getenv('LEADER_PORTS', "6669")
        )

        self.leader_request_timeout =config.distributed_config.get(
            'leader_request_timeout',
            30
        )

        leader_addresses = config.distributed_config.get(
            'leader_addresses',
            os.getenv('LEADER_ADDRESSES')
        )

        if leader_addresses:
            leader_addresses = leader_addresses.split(",")
            self.leader_addresses = leader_addresses

        else:
            self.leader_ips = self.leader_ips.split(",")
            self.leader_ports = [int(port) for port in self.leader_ports.split(",")]

            self.leader_addresses = []

            for leader_ip, leader_port in zip(self.leader_ips, self.leader_ports):
                self.leader_addresses.append(
                    f'{leader_ip}:{leader_port}'
                )

        leader_address_strings = ''.join([f'\n- {leader_address}' for leader_address in self.leader_addresses])
        
        self.session_logger.info(f'\nConnecting to leaders at addresses: {leader_address_strings}\n')

        self.registry = LeaderRegistry(self.leader_addresses)
        self.broadkast_manager = BootstrapManager(config)

    async def initialize(self):
        await self.registry.initialize()

    async def registered(self):
        return self.registry.leader_addresses

    async def add_new_leaders(self, new_leaders_queue):
        
        has_new_leaders = await new_leaders_queue.not_empty()
        if has_new_leaders:        
            await self.register_worker(leaders=new_leaders_queue)

        return new_leaders_queue

    async def register_worker(self, leaders=None):

        if leaders is None:
            if self.broadkast_manager.use_bootstrap:
                await self.broadkast_manager.client.discover()
                leaders = self.broadkast_manager.client.registry

            else:
                leaders = self.registry

        responses, _ = await asyncio.wait([
            self._register_worker(
                leaders[leader_address]
            ) for leader_address in leaders.leader_addresses
        ], timeout=self.leader_request_timeout)

        results = await asyncio.gather(*responses)
        
        for registration_response in results:
            await self.registry.register_leader(registration_response.host_address)

        self.session_logger.debug('Worker successfully registered.')

    @connect_with_retry_async(wait_buffer=5, timeout_threshold=15)
    async def _register_worker(self, leader):
        register_request = WorkerRegisterRequest(
            host_address=self.worker_address,
            host_id=str(self.worker_id),
            server_address=self.worker_server_address
        )

        return await leader.client.AddClient(register_request)

    async def deregister_worker(self, executor):
        
        self.session_logger.debug('Deregistring client...')

        await asyncio.wait([
            await self._deregister_worker(
                self.registry[leader_address], 
                executor.handler.reporter.fields
            ) for leader_address in self.registry.registered_leaders
        ], timeout=self.leader_request_timeout)

        self.session_logger.info('Closing down worker. Goodbye!')

    @connect_with_retry_async(wait_buffer=5, timeout_threshold=15)
    async def _deregister_worker(self, leader, reporter_fields):
        deregister_request = WorkerDeregisterRequest(
            host_address=self.worker_address,
            host_id=str(self.worker_id),
        )

        deregister_request.reporter_fields.extend(reporter_fields)

        await leader.client.RemoveClient(deregister_request)

        await self.registry.remove_leader(leader.address)

    async def check_leader_heartbeat(self):

        self.session_logger.debug('Getting leader heartbeats...')

        for leader in self.registry:
            heartbeat_response = await self._check_leader_heartbeat(leader)
            await self._update_registry(heartbeat_response,leader)
            
    @connect_or_return_none(wait_buffer=1, timeout_threshold=1)
    async def _check_leader_heartbeat(self, leader):
        heartbeat_request = LeaderHeartbeatRequest(
            host_address=self.worker_address,
            host_id=str(self.worker_id),
            status='OK'
        )

        return await leader.client.CheckLeaderHeartbeat(heartbeat_request)

    async def _update_registry(self, leader_response, leader):

        if leader_response and isinstance(leader_response, grpc.aio.AioRpcError) is False:
            
            await self.registry.register_leader(leader_response.host_address)
            await self.register_worker()

            id_not_set = await self.registry.leader_id_not_set(leader_response.host_address)
            if id_not_set:
                await self.registry.set_leader_id(leader_response.host_address, leader_response.host_id)

        else:
            await self.registry.remove_leader(leader.leader_address)

