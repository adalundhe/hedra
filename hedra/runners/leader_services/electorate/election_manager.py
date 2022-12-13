import asyncio
import time
import os

from hedra.reporting import Handler
from hedra.runners.leader_services.bootstrap import BootstrapManager
from hedra.runners.leader_services.leader_registry import LeaderRegistry
from hedra.runners.leader_services.leader_registry.leader_service import LeaderService

class ElectionManager:


    def __init__(self) -> None:

        logger = Logger()
        self.session_logger = logger.generate_logger()
        self.expected_leaders = []
        self.polls_received = 0
        self.is_leader = False
        self.leader_ip = None
        self.leader_port = None
        self.leader_address = None
        self.leader_id = None
        self.registered_services = None
        self.leader_election_timeout = None
        self.discovery_mode = None
        self.loop = asyncio.get_event_loop()
        
    async def setup(self, config, leader_id):
        self.config = config
        self.reporter_config = config.reporter_config

        self.leader_id = leader_id

        self.leader_election_timeout = config.distributed_config.get(
            'leader_election_timeout', 
            60
        )

        self.elected_leader_id = self.leader_id
        self.leaders_count = len(self.expected_leaders)
        self.elected_leader = None
        self.poll_completed = False

    async def register(self, leader_registration_request):
        await self.registered_services.register(leader_registration_request)
        return {
            'host_address': self.leader_address,
            'host_id': str(self.leader_id)
        }

    async def elect_leader_and_submit(self, job_results, job_id=None):

        handler = Handler(self.config)
        await handler.initialize_reporter()

        if job_id:
            self.session_logger.info(f'\nJob - {job_id} has completed...')

        else:
            self.session_logger.info('\nAll workers have signaled completion, ending session...')

        submitted = False
        elapsed = 0

        start = time.time()
        overall_timeout = self.config.distributed_config.get('leader_results_timeout', 60)
        
        if self.leaders_count > 0:
            while submitted is False and elapsed < overall_timeout:

                registration_complete = await self.registered_services.registration_complete()
                if registration_complete is False:
                    self.session_logger.info('\nElecting leader to submit results...')
                    await self.register_leaders()
                    await self.wait_until_done()

                self.elected_leader = await self.elect_leader()

                if self.elected_leader:
                    self.session_logger.info(f'Elected leader at - {self.elected_leader.service_address} - to submit results.')

                if self.is_leader:
                    await handler.merge(job_results)
                    stats = await handler.get_stats()

                    self.session_logger.info(f'Leader - {self.leader_address} - submitting session results...')
                    submitted = await handler.submit(stats)
                    self.session_logger.info(f'Results submitted!\n')
                    self.poll_completed = submitted
                    await self.wait_until_poll_complete()

                else:

                    completed = await self.poll_leader()
                    if completed:
                        submitted = True

                        self.session_logger.info('Poll complete.\n')

                    else:
                        await self.remove_elected_leader()

                        self.session_logger.info(f'Leader at - {self.elected_leader.service_address} - failed to respond. Electing new leader.')

                elapsed = time.time() - start

        if submitted is False:
            self.session_logger.info(f'Elected leader at - {self.leader_address} - to submit results.\n')
            await handler.merge(job_results)
            stats = await handler.get_stats()
            await handler.submit(stats)
            self.session_logger.info(f'Results submitted!\n')

    async def elect_leader(self):

        registration_complete = await self.registered_services.registration_complete()

        if registration_complete:
            elected_leader = await self.registered_services.find_min_id()
            await self.registered_services.set_elected_leader(elected_leader)
            self.elected_leader_id = self.registered_services.elected_leader.service_id

            if self.elected_leader_id.int == self.leader_id.int:
                self.is_leader = True

        return self.registered_services.elected_leader

    async def remove_elected_leader(self):
        if self.registered_services.elected_leader:
            await self.registered_services.remove_service(self.registered_services.elected_leader.service_address)

    async def wait_until_done(self):
        elapsed = 0
        start = time.time()
        wait_interval = self.config.distributed_config.get('leader_wait_interval', 1)

        registration_complete = await self.registered_services.registration_complete()

        while registration_complete is False and elapsed < self.leader_election_timeout:
            registration_complete = await self.registered_services.registration_complete()
            await asyncio.sleep(wait_interval)
            elapsed = time.time() - start

        return await self.registered_services.registration_complete()

    async def register_leaders(self, config):
        await self._discover_leaders(config)
        
        async for service in self.registered_services.services():
            is_registered = await service.is_registered(self.registered_services)
            
            if is_registered is False:
                response = await service.register(self.leader_address, self.leader_id)
                if response:
                    await self.registered_services.register_by_response(response)

    async def setup_leader_addresses(self, config):
        leader_ips = config.distributed_config.get(
            'leader_ips',
            os.getenv('LEADER_IPS')
        )

        leader_ports = config.distributed_config.get(
            'leader_ports',
            os.getenv('LEADER_PORTS', '6669')
        )

        leader_addresses = config.distributed_config.get(
            'leader_addresses',
            os.getenv('LEADER_ADDRESSES')
        )

        self.discovery_mode = config.distributed_config.get(
            'discovery_mode',
            os.getenv('DISCOVERY_MODE', 'static')
        )

        if leader_addresses:
            self.expected_leaders = leader_addresses.split(',')
            leader_ips = []
            leader_ports = []

            for leader_address in self.expected_leaders:
                leader_ip, leader_port = leader_address.split(':')
                leader_ips.append(leader_ip)
                leader_ports.append(int(leader_port))

        elif leader_ips:
            leader_ips = leader_ips.split(',')
            leader_ports = leader_ports.split(',')

            for ip, port in zip(leader_ips, leader_ports):
                self.expected_leaders.append(f'{ip}:{port}')
        
        else:

            if leader_ips is None:
                leader_ips = ['localhost']

            leader_ports = leader_ports.split(',')
            self.expected_leaders = [f'{leader_ips[0]}:{leader_ports[0]}']

        self.leader_ip = leader_ips.pop()
        self.leader_port = leader_ports.pop()
        self.leader_address = self.expected_leaders.pop()

    async def _discover_leaders(self, config):

        bootstrap_manager = BootstrapManager(config)
        self.registered_services = LeaderRegistry()

        if self.discovery_mode == 'kubernetes':
            self.session_logger.info('\nBeginning dynamic Kubernetes leader discovery.')

            await bootstrap_manager.client.discover(
                host_address=self.leader_address
            )

            self.session_logger.info('Leader discovery complete!\n')

            discovered = await bootstrap_manager.discovered()
            await self.registered_services.setup(discovered)
            self.leader_id = self.registered_services.leader_id

        elif self.discovery_mode == 'dynamic':
            self.session_logger.info('\nBeginning dynamic leader discovery.')

            await bootstrap_manager.register_leader(self.leader_address)
            await bootstrap_manager.wait_for_leaders(
                self.leader_address,
                self.leader_id
            )

        else:
            host = LeaderService(
                self.leader_address,
                self.leader_id,
                host=True
            )

            services = [
                LeaderService(
                    service_address,
                    None,
                    host=False
                ) for service_address in self.expected_leaders
            ]

            await self.registered_services.setup([
                *services,
                host
            ])
    
    async def wait_until_poll_complete(self):
        poll_timeout = self.config.distributed_config.get('leader_poll_timeout', 15)
        wait_interval = self.config.distributed_config.get('leader_wait_interval', 1)

        elapsed = 0
        start = time.time()

        while self.polls_received < self.leaders_count and elapsed < poll_timeout:
            await asyncio.sleep(wait_interval)

            elapsed = time.time() - start
                    
    async def poll_leader(self):
        poll_timeout = self.config.distributed_config.get('leader_poll_timeout', 15)
        wait_interval = self.config.distributed_config.get('leader_wait_interval', 1)

        elapsed = 0
        start = time.time()
        completed = False

        while completed is False and elapsed < poll_timeout:
            if self.registered_services.registered_count <= 1:
                return False

            response = await self.elected_leader.poll()
            if response:
                completed = response.completed

            await asyncio.sleep(wait_interval)
            elapsed = time.time() - start

        return completed


