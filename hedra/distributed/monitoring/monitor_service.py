import asyncio
import math
import random
import time
from collections import defaultdict, deque
from hedra.distributed.env import (
    Env, 
    MonitorEnv,
    load_env
)
from hedra.distributed.env.time_parser import TimeParser
from hedra.distributed.hooks.client_hook import client
from hedra.distributed.hooks.server_hook import server
from hedra.distributed.models.healthcheck import HealthCheck, HealthStatus
from hedra.distributed.service.controller import Controller
from hedra.distributed.snowflake import Snowflake
from hedra.distributed.types import Call
from typing import Optional, Dict, Tuple, List, Deque, Union


async def cancel(pending_item: asyncio.Task) -> None:
    pending_item.cancel()
    if not pending_item.cancelled():
        try:
            await pending_item

        except asyncio.CancelledError:
            pass

        except asyncio.IncompleteReadError:
            pass


class Monitor(Controller):

    def __init__(
        self,
        host: str,
        port: int,
        env: Env,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        workers: int=0,
    ) -> None:
        
        if workers <= 1:
            engine = 'async'

        else:
            engine = 'process'

        if env is None:
            env: Env = load_env(Env)
            
        monitor_env: MonitorEnv = load_env(MonitorEnv)

        super().__init__(
            host,
            port,
            cert_path=cert_path,
            key_path=key_path,
            workers=workers,
            env=env,
            engine=engine
        )

        self.status: HealthStatus = 'initializing'
        

        self.error_context: Optional[str] = None

        self.registration_timeout = TimeParser(
            monitor_env.MERCURY_SYNC_REGISTRATION_TIMEOUT
        ).time

        self.boot_wait = TimeParser(
            monitor_env.MERCURY_SYNC_BOOT_WAIT
        ).time
        
        self._healthchecks: Dict[str, asyncio.Task] = {}
        self._registered: Dict[int, Tuple[str, int]] = {}
        self._running = False

        self._cleanup_interval = TimeParser(
            env.MERCURY_SYNC_CLEANUP_INTERVAL
        ).time

        self._poll_interval = TimeParser(
            monitor_env.MERCURY_SYNC_HEALTH_POLL_INTERVAL
        ).time

        self._poll_timeout = TimeParser(
            monitor_env.MERCURY_SYNC_HEALTH_CHECK_TIMEOUT
        ).time

        self._reboot_timeout = TimeParser(
            monitor_env.MERCURY_SYNC_IDLE_REBOOT_TIMEOUT
        ).time

        self._max_time_idle = TimeParser(
            monitor_env.MERCURY_SYNC_MAX_TIME_IDLE
        ).time

        self._poll_retries = monitor_env.MERCURY_SYNC_MAX_POLL_MULTIPLIER

        self._sync_interval = TimeParser(
            monitor_env.MERCURY_SYNC_UDP_SYNC_INTERVAL
        ).time

        self._check_nodes_count = monitor_env.MERCURY_SYNC_INDIRECT_CHECK_NODES

        self.min_suspect_multiplier = monitor_env.MERCURY_SYNC_MIN_SUSPECT_TIMEOUT_MULTIPLIER
        self.max_suspect_multiplier = monitor_env.MERCURY_SYNC_MAX_SUSPECT_TIMEOUT_MULTIPLIER 
        self._min_suspect_node_count = monitor_env.MERCURY_SYNC_MIN_SUSPECT_NODES_THRESHOLD
        self._max_poll_multiplier = monitor_env.MERCURY_SYNC_MAX_POLL_MULTIPLIER
        self._local_health_multiplier = 0

        self._confirmed_suspicions: Dict[Tuple[str, int], int] = defaultdict(lambda: 0)
        self._waiter: Union[asyncio.Future, None] = None

        self._tasks_queue: Deque[asyncio.Task] = deque()
        self._degraded_nodes: Deque[Tuple[str, int]] = deque()
        self._suspect_nodes: Deque[Tuple[str, int]] = deque()

        self._degraded_tasks: Dict[Tuple[str, int], asyncio.Task] = {}
        self._suspect_tasks: Dict[Tuple[str, int], asyncio.Task] = {}
        self._latest_update: Dict[Tuple[str,int], int] = {}

        self._local_health_monitor: Union[asyncio.Task, None] = None
        self._udp_sync_task: Union[asyncio.Task, None] = None
        self._tcp_sync_task: Union[asyncio.Task, None] = None

        self._cleanup_task: Union[asyncio.Task, None] = None
        self._investigating_nodes: Dict[Tuple[str, int], Dict[Tuple[str, int]]] = defaultdict(dict)
        self._node_statuses: Dict[Tuple[str, int], HealthStatus] = {}

        self.bootstrap_host: Union[str, None] = None
        self.bootstrap_port: Union[int, None] = None

        self._healthy_statuses = [
            'waiting',
            'healthy'
        ]

        self._unhealthy_statuses = [
            'suspect',
            'failed'
        ]

    @server()
    async def update_node_status(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        
        update_node_host = healthcheck.source_host
        update_node_port = healthcheck.source_port
        update_status = healthcheck.status

        if healthcheck.target_host and healthcheck.target_port:
            update_node_host = healthcheck.target_host
            update_node_port = healthcheck.target_port

        if healthcheck.target_status:
            update_status = healthcheck.target_status
        
        local_status = self._node_statuses.get((
            update_node_host,
            update_node_port
        ))


        target_last_updated: Union[int, None] = healthcheck.target_last_updated
        local_last_updated: Union[int, None] = self._latest_update.get((
            update_node_host,
            update_node_port
        ), 0)

        not_self = update_node_host != self.host and update_node_port != self.port

        if not_self and local_status in self._unhealthy_statuses:

            self._tasks_queue.append(
                asyncio.create_task(
                    self.refresh_clients(
                        HealthCheck(
                            host=update_node_host,
                            port=update_node_port,
                            source_host=self.host,
                            source_port=self.port,
                            status=update_status,
                            error=healthcheck.error
                        )
                    )
                )
            )

        elif not_self and local_status is None:

            self._tasks_queue.append(
                asyncio.create_task(
                    self.extend_client(
                        HealthCheck(
                            host=update_node_host,
                            port=update_node_port,
                            source_host=self.host,
                            source_port=self.port,
                            status=self.status,
                            error=healthcheck.error
                        )
                    )
                )
            )

        if target_last_updated > local_last_updated:
            self._node_statuses[(update_node_host, update_node_port)] = update_status

        return HealthCheck(
            host=healthcheck.source_host,
            port=healthcheck.source_port,
            source_host=self.host,
            source_port=self.port,
            status=self.status
        )

    @server()
    async def update_as_suspect(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        
        source_host = healthcheck.source_host
        source_port = healthcheck.source_port

        if self.status == 'healthy':
            self._local_health_multiplier = min(
                self._local_health_multiplier, 
                self._max_poll_multiplier
            ) + 1

            self._tasks_queue.append(
                asyncio.create_task(
                    self._run_healthcheck(
                        source_host,
                        source_port
                    )
                )
            )

        return HealthCheck(
            host=source_host,
            port=source_port,
            source_host=self.host,
            source_port=self.port,
            status=self.status
        )

    @server()
    async def send_indirect_check(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        
        source_host = healthcheck.source_host
        source_port = healthcheck.source_port

        target_host = healthcheck.target_host
        target_port = healthcheck.target_port

        try:

            investigation_update = self._acknowledge_indirect_probe(
                source_host,
                source_port,
                target_host,
                target_port
            )

            indirect_probe = self._run_healthcheck(
                target_host,
                target_port
            )

            for task in asyncio.as_completed([
                investigation_update,
                indirect_probe
            ]):
                await task

                self._local_health_multiplier = max(
                    self._local_health_multiplier - 1, 
                    0
                )

            # We've received a refutation
            return HealthCheck(
                host=healthcheck.source_host,
                port=healthcheck.source_port,
                target_status=self._node_statuses.get((target_host, target_port)),   
                source_host=target_host,
                source_port=target_port,
                status=self.status,
                error=self.error_context
            )

        except asyncio.TimeoutError:
            self._local_health_multiplier = min(
                self._local_health_multiplier, 
                self._max_poll_multiplier
            ) + 1

            # Our suspicion is correct!
            return HealthCheck(
                host=healthcheck.source_host,
                port=healthcheck.source_port,
                source_host=target_host,
                source_port=target_port,
                target_status='suspect', 
                status=self.status
            )
    
    @server()
    async def update_acknowledged(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        source_host = healthcheck.source_host
        source_port = healthcheck.source_port
        target_host = healthcheck.target_host
        target_port = healthcheck.target_port

        if self._investigating_nodes.get((target_host, target_port)) is None:
            self._investigating_nodes[(target_host, target_port)] = {}

        self._investigating_nodes[(target_host, target_port)].update({
            (source_host, source_port): healthcheck.status
        })

        return HealthCheck(
            host=source_host,
            port=source_port,
            source_host=self.host,
            source_port=self.port,
            status=self.status
        )
        
    @server()
    async def register_health_update(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
            
        source_host = healthcheck.source_host
        source_port = healthcheck.source_port

        target_host = healthcheck.target_host
        target_port = healthcheck.target_port

        local_node_status = self._node_statuses.get((source_host, source_port))

        suspect_tasks = dict(self._suspect_tasks)
        suspect_task = suspect_tasks.pop((source_host, source_port), None)

        if suspect_task:
            self._tasks_queue.append(
                asyncio.create_task(
                    cancel(suspect_task)
                )
            )

            del self._suspect_tasks[(source_host, source_port)]

            self._suspect_tasks = suspect_tasks


        if local_node_status is None:

            self._tasks_queue.append(
                asyncio.create_task(
                    self.extend_client(
                        HealthCheck(
                            host=source_host,
                            port=source_port,
                            source_host=self.host,
                            source_port=self.port,
                            status=healthcheck.status,
                            error=healthcheck.error
                        )
                    )
                )
            )

        elif local_node_status in self._unhealthy_statuses:
            self._tasks_queue.append(
                asyncio.create_task(
                    self.refresh_clients(
                        HealthCheck(
                            host=source_host,
                            port=source_port,
                            source_host=self.host,
                            source_port=self.port,
                            status=healthcheck.status,
                            error=healthcheck.error
                        )
                    )
                )
            )
    

        self._node_statuses[(source_host, source_port)] = healthcheck.status
        self._latest_update[(source_host, source_port)] = Snowflake.parse(shard_id).timestamp

        if target_host and target_port: 
            self._node_statuses[(target_host, target_port)] = healthcheck.target_status

        return HealthCheck(
            host=healthcheck.source_host,
            port=healthcheck.source_port,
            source_host=self.host,
            source_port=self.port,
            source_status=local_node_status,
            error=self.error_context,
            status=self.status
        )

    @client('register_health_update')
    async def push_health_update(
        self,
        host: str,
        port: int,
        health_status: HealthStatus,
        target_host: Optional[str]=None,
        target_port: Optional[str]=None,
        error_context: Optional[str]=None
    ) -> Call[HealthCheck]:
        
        target_status: Union[HealthCheck, None] = None
        if target_host and target_port:
            target_status = self._node_statuses.get((target_host, target_port))

        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            target_host=target_host,
            target_port=target_port,
            target_status=target_status,
            error=error_context,
            status=health_status
        )
    
    @client('register_health_update', as_tcp=True)
    async def push_tcp_health_update(
        self,
        host: str,
        port: int,
        health_status: HealthStatus,
        target_host: Optional[str]=None,
        target_port: Optional[str]=None,
        error_context: Optional[str]=None
    ) -> Call[HealthCheck]:
        
        target_status: Union[HealthCheck, None] = None
        if target_host and target_port:
            target_status = self._node_statuses.get((target_host, target_port))

        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            target_host=target_host,
            target_port=target_port,
            target_status=target_status,
            error=error_context,
            status=health_status
        )
    
    async def _run_tcp_healthcheck(
        self, 
        host: str, 
        port: int,
        target_host: Optional[str]=None,
        target_port: Optional[str]=None
    ) -> Union[Tuple[int, HealthCheck], None]:
        
        shard_id: Union[int, None] = None
        healthcheck: Union[HealthCheck, None] = None

        for _ in range(self._poll_retries):

            try:

                response: Tuple[int, HealthCheck] = await asyncio.wait_for(
                    self.push_tcp_health_update(
                        host,
                        port,
                        self.status,
                        target_host=target_host,
                        target_port=target_port,
                        error_context=self.error_context
                    ),
                    timeout=self._poll_timeout * (self._local_health_multiplier + 1)
                )

                shard_id, healthcheck = response
                source_host, source_port = healthcheck.source_host, healthcheck.source_port

                self._node_statuses[(source_host, source_port)] = healthcheck.status

                self._local_health_multiplier = max(
                    0, 
                    self._local_health_multiplier - 1
                )

                return shard_id, healthcheck

            except (asyncio.TimeoutError, RuntimeError):
                self._local_health_multiplier = min(
                    self._local_health_multiplier, 
                    self._max_poll_multiplier
                ) + 1
                
        check_host = host
        check_port = port

        if target_host and target_port:
            check_host = target_host
            check_port = target_port

        if healthcheck is None and self._node_statuses.get((check_host, check_port)) == 'healthy':
            self._node_statuses[(check_host, check_port)] = 'suspect'

            self._suspect_nodes.append((
                check_host,
                check_port
            ))

            self._suspect_tasks[(host, port)] = asyncio.create_task(
                self._start_suspect_monitor()
            )

        return shard_id, healthcheck
    
    @client('update_acknowledged')
    async def push_acknowledge_check(
        self,
        host: str,
        port: int,
        target_host: str,
        target_port: int,
        health_status: HealthStatus,
        error_context: Optional[str]=None
    ) -> Call[HealthCheck]:
        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            target_host=target_host,
            target_port=target_port,
            status=health_status,
            error=error_context
        )
    
    @client('send_indirect_check')
    async def request_indirect_check(
        self,
        host: str,
        port: int,
        target_host: str,
        target_port: int,
        health_status: HealthStatus,
        error_context: Optional[str]=None

    ) -> Call[HealthCheck]:
        return HealthCheck(
            host=host,
            port=port,
            target_host=target_host,
            target_port=target_port,
            target_status=self._node_statuses[(target_host, target_port)],
            source_host=self.host,
            source_port=self.port,
            error=error_context,
            status=health_status
        )
    
    @client('update_node_status')
    async def push_status_update(
        self,
        host: str,
        port: int,
        health_status: HealthStatus,
        target_host: Optional[str]=None,
        target_port: Optional[int]=None,
        error_context: Optional[str]=None
    ) -> Call[HealthCheck]:
        
        target_status: Union[HealthStatus, None] = None
        target_last_updated: Union[int,  None] = self._latest_update.get(
            (host, port), 0
        )

        if target_host and target_port:
            target_status = self._node_statuses.get((target_host, target_port))
            target_last_updated = self._latest_update.get(
                (target_host, target_port), 0
            )

        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            target_host=target_host,
            target_port=target_port,
            target_last_updated=target_last_updated,
            target_status=target_status,
            status=health_status,
            error=error_context
        )
    
    @client('update_node_status', as_tcp=True)
    async def push_tcp_status_update(
        self,
        host: str,
        port: int,
        health_status: HealthStatus,
        target_host: Optional[str]=None,
        target_port: Optional[int]=None,
        error_context: Optional[str]=None
    ) -> Call[HealthCheck]:
        
        target_status: Union[HealthStatus, None] = None
        target_last_updated: Union[int,  None] = self._latest_update.get(
            (host, port), 0
        )

        if target_host and target_port:
            target_status = self._node_statuses.get((target_host, target_port))
            target_last_updated = self._latest_update.get(
                (target_host, target_port), 0
            )


        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            target_host=target_host,
            target_port=target_port,
            target_status=target_status,
            target_last_updated=target_last_updated,
            status=health_status,
            error=error_context
        )
    
    @client('update_as_suspect')
    async def push_suspect_update(
        self,
        host: str,
        port: int,
        health_status: HealthStatus,
        error_context: Optional[str]=None
    ) -> Call[HealthCheck]:
        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            status=health_status,
            error=error_context
        )
    
    async def start(self):

        await self.start_server()
        await asyncio.sleep(self.boot_wait)

    async def register(
        self,
        host: str,
        port: int
    ):
        
        self.bootstrap_host = host
        self.bootstrap_port = port
        
        await asyncio.wait_for(
            asyncio.create_task(
                self.start_client(
                    HealthCheck(
                        host=host,
                        port=port,
                        source_host=self.host,
                        source_port=self.port,
                        status=self.status
                    ),
                    cert_path=self.cert_path,
                    key_path=self.key_path
                )
            ),
            timeout=self.registration_timeout
        )

        self._node_statuses[(host, port)] = 'healthy'

        self.status = 'healthy'
        self._running = True
        
        self._healthchecks[(host, port)] = asyncio.create_task(
            self.start_health_monitor()
        )
        
        self.confirmation_task = asyncio.create_task(
            self.cleanup_pending_checks()
        )

        self._udp_sync_task = asyncio.create_task(
            self._run_udp_state_sync()
        )

        self._tcp_sync_task = asyncio.create_task(
            self._run_tcp_state_sync()
        )

    def _calculate_min_suspect_timeout(self):
        nodes_count = len(self._node_statuses) + 1

        poll_interval = self._poll_interval * (self._local_health_multiplier + 1)

        return round(
            self.min_suspect_multiplier * math.log10(nodes_count) * poll_interval,
            2
        )

    def _calculate_max_suspect_timeout(self, min_suspect_timeout: float):
        
        return round(
            self.max_suspect_multiplier * min_suspect_timeout,
            2
        )

    def _calculate_suspicion_timeout(
        self,
        suspect_node_address: Tuple[str, int]
    ):

        min_suspect_timeout = self._calculate_min_suspect_timeout()
        
        max_suspect_timeout = self._calculate_max_suspect_timeout(min_suspect_timeout)

        confirmed_suspect_count = max(
            0,
            self._confirmed_suspicions[suspect_node_address]
        )

        timeout_modifier = math.log(
            confirmed_suspect_count + 1
        )/math.log(self._min_suspect_node_count + 1)

        timeout_difference = max_suspect_timeout - min_suspect_timeout

        return max(
            min_suspect_timeout,
            max_suspect_timeout - (timeout_difference * timeout_modifier)
        )
    
    async def _acknowledge_indirect_probe(
        self,
        host: str,
        port: int,
        target_host: str,
        target_port: int
    ):
        try:
            timeout = self._poll_timeout * (self._local_health_multiplier + 1)
            response: Tuple[int, HealthCheck] = await asyncio.wait_for(
                self.push_acknowledge_check(
                    host,
                    port,
                    target_host,
                    target_port,
                    self.status,
                    error_context=self.error_context
                ),
                timeout=timeout
            )

            _, healthcheck = response

            source_host, source_port = healthcheck.source_host, healthcheck.source_port

            self._node_statuses[(source_host, source_port)] = healthcheck.status

            self._local_health_multiplier = max(
                0, 
                self._local_health_multiplier - 1
            )

        except asyncio.TimeoutError:
            self._local_health_multiplier = min(
                self._local_health_multiplier, 
                self._max_poll_multiplier
            ) + 1

            if self._node_statuses.get((host, port)) == 'healthy':
                self._node_statuses[(host, port)] = 'suspect'

                self._suspect_nodes.append((
                    host,
                    port
                ))

                self._suspect_tasks[(host, port)] = asyncio.create_task(
                    self._start_suspect_monitor()
                )

    async def _run_healthcheck(
        self, 
        host: str, 
        port: int,
        target_host: Optional[str]=None,
        target_port: Optional[str]=None
    ) -> Union[Tuple[int, HealthCheck], None]:
        
        shard_id: Union[int, None] = None
        healthcheck: Union[HealthCheck, None] = None

        for _ in range(self._poll_retries):

            try:

                response: Tuple[int, HealthCheck] = await asyncio.wait_for(
                    self.push_health_update(
                        host,
                        port,
                        self.status,
                        target_host=target_host,
                        target_port=target_port,
                        error_context=self.error_context
                    ),
                    timeout=self._poll_timeout * (self._local_health_multiplier + 1)
                )

                shard_id, healthcheck = response
                source_host, source_port = healthcheck.source_host, healthcheck.source_port

                self._node_statuses[(source_host, source_port)] = healthcheck.status

                self._local_health_multiplier = max(
                    0, 
                    self._local_health_multiplier - 1
                )

                return shard_id, healthcheck

            except asyncio.TimeoutError:
                self._local_health_multiplier = min(
                    self._local_health_multiplier, 
                    self._max_poll_multiplier
                ) + 1

        check_host = host
        check_port = port

        if target_host and target_port:
            check_host = target_host
            check_port = target_port

        if healthcheck is None and self._node_statuses.get((check_host, check_port)) == 'healthy':
            self._node_statuses[(check_host, check_port)] = 'suspect'

            self._suspect_nodes.append((
                check_host,
                check_port
            ))

            self._suspect_tasks[(host, port)] = asyncio.create_task(
                self._start_suspect_monitor()
            )

        return shard_id, healthcheck

    async def _start_suspect_monitor(self):

        if len(self._suspect_nodes) < 1:
            return
        
        address = self._suspect_nodes.pop()
        suspect_host, suspect_port = address
        
        suspicion_timeout = self._calculate_suspicion_timeout(address)


        elapsed = 0
        start = time.monotonic()

        while elapsed < suspicion_timeout and self._node_statuses[(suspect_host, suspect_port)] == 'suspect':

            self._tasks_queue.append(
                asyncio.create_task(
                    self._push_suspect_update(
                        host=suspect_host,
                        port=suspect_port,
                        health_status=self.status,
                        error_context=self.error_context
                    )
                )
            )
                
            confirmation_members = self._get_confirmation_members((
                suspect_host,
                suspect_port
            ))

            suspect_count = await self._request_indirect_probe(
                suspect_host,
                suspect_port,
                confirmation_members
            )

            self._confirmed_suspicions[(suspect_host, suspect_port)] += max(
                0,
                suspect_count - 1
            )

            indirect_ack_count = len(self._investigating_nodes[(suspect_host, suspect_port)])

            missing_ack_count = len(confirmation_members) - indirect_ack_count

            next_health_multiplier = self._local_health_multiplier + missing_ack_count - indirect_ack_count
            if next_health_multiplier < 0:
                self._local_health_multiplier = 0

            else:
                self._local_health_multiplier = min(
                    next_health_multiplier, 
                    self._max_poll_multiplier
                ) + 1

            confirmation_members_count = len(confirmation_members)

            if suspect_count < confirmation_members_count:
                # We had a majority confirmation the node was healthy.
                self._investigating_nodes[(suspect_host, suspect_port)] = {}
                self._confirmed_suspicions[(suspect_host, suspect_port)] = 0

                self._node_statuses[(suspect_host, suspect_port)] = 'healthy'

                break
            
            await asyncio.sleep(
                self._poll_interval * (self._local_health_multiplier + 1)
            ) 

            elapsed = time.monotonic() - start
            suspicion_timeout = self._calculate_suspicion_timeout(address)

        if self._node_statuses[(suspect_host, suspect_port)] == 'suspect':
            self._node_statuses[(suspect_host, suspect_port)] = 'failed'
        
        self._investigating_nodes[(suspect_host, suspect_port)] = {}
        self._confirmed_suspicions[(suspect_host, suspect_port)] = 0
            
    def _get_confirmation_members(self, suspect_address: Tuple[str, int]) -> List[Tuple[str, int]]:

        confirmation_members = [
            address for address in self._node_statuses.keys() if address != suspect_address
        ]

        confirmation_members_count = len(confirmation_members)

        if self._check_nodes_count > confirmation_members_count:
            self._check_nodes_count = confirmation_members_count
            
        confirmation_members = random.sample(
            confirmation_members, 
            self._check_nodes_count
        )

        return confirmation_members

    async def _request_indirect_probe(
        self,
        host: str,
        port: int,
        confirmation_members: List[Tuple[str, int]]
    ) -> Tuple[List[Call[HealthCheck]], int]:

        if len(confirmation_members) < 1:
            requested_checks = [
                asyncio.create_task(
                    self._run_tcp_healthcheck(
                        host,
                        port
                    )
                )
            ]
        
        else:
            requested_checks = [
                asyncio.create_task(
                    self.request_indirect_check(
                        node_host,
                        node_port,
                        host,
                        port,
                        self.status,
                        error_context=self.error_context
                    )
                ) for node_host, node_port in confirmation_members
            ]

            requested_checks.append(
                asyncio.create_task(
                    self._run_tcp_healthcheck(
                        host,
                        port
                    )
                )
            )

        check_tasks: Tuple[List[asyncio.Task], List[asyncio.Task]] = await asyncio.wait(
            requested_checks, 
            timeout=self._poll_timeout * (self._local_health_multiplier + 1)
        )

        completed, pending = check_tasks

        results: List[Call[HealthCheck]]  = await asyncio.gather(
            *completed,
            return_exceptions=True
        )

        healthchecks = [
            result for result in results if isinstance(
                result,
                tuple
            )
        ]

        errors = [
            result for result in results if isinstance(
                result,
                tuple
            ) is False
        ]

        sorted_checks: List[Call[HealthCheck]] = list(sorted(
            healthchecks,
            key=lambda check: Snowflake.parse(
                Snowflake.parse(check[0]).timestamp
            ).timestamp
        ))

        suspect = [
            (
                shard_id,
                check
            ) for shard_id, check in sorted_checks if check.target_status == 'suspect'
        ]

        healthy = [
            (
                shard_id,
                check
            ) for shard_id, check in sorted_checks if check.target_status == 'healthy'
        ]
            
        if len(healthy) < 1:
            suspect_count = len(suspect) + len(pending) + len(errors)

        else:
            suspect_checks: List[Call[HealthCheck]] = []
            for suspect_shard_id, suspect_check in suspect:

                newer_count = 0
                for healthy_shard_id, _ in healthy:
                    if suspect_shard_id > healthy_shard_id:
                        newer_count += 1

                if newer_count >= len(healthy):
                    suspect_checks.append((
                        suspect_shard_id,
                        suspect_check
                    ))

            suspect_count = len(suspect_checks) + len(pending) + len(errors)
        
        await asyncio.gather(*[
            cancel(pending_check) for pending_check in pending
        ])

        return suspect_count
    
    async def _propagate_state_update(
        self,
        target_host: str,
        target_port: int
    ):
        monitoring = [
            address for address, status in self._node_statuses.items() if status in self._healthy_statuses
        ]

        for host, port in monitoring:
            await self.push_health_update(
                host,
                port,
                self.status,
                target_host=target_host,
                target_port=target_port
            )

    async def run_forever(self):
        self._waiter = asyncio.Future()
        await self._waiter

    async def start_health_monitor(self):

        while self._running:

            monitors = list(self._node_statuses.keys())

            host: Union[str, None] = None
            port: Union[int, None] = None

            monitors_count = len(monitors)

            if monitors_count > 0:
                host, port = random.choice(monitors)

            if self._node_statuses.get((host, port)) == 'healthy':
        
                self._tasks_queue.append(
                    asyncio.create_task(
                        self._run_healthcheck(
                            host,
                            port
                        )
                    )
                )

            await asyncio.sleep(
                self._poll_interval * (self._local_health_multiplier + 1)
            )

    async def _run_udp_state_sync(self):
        while self._running:

            monitors = [
                address for address, status in self._node_statuses.items() if status in self._healthy_statuses
            ]

            active_nodes_count = len(monitors)

            if active_nodes_count > 0:

                self._tasks_queue.extend([
                    asyncio.create_task(
                        self._push_state_to_node(
                            host=host,
                            port=port
                        )
                    ) for host, port in monitors
                ])

            await asyncio.sleep(
                self._sync_interval
            )

    async def _run_tcp_state_sync(self):

        await asyncio.sleep(
            self._sync_interval/2
        )

        while self._running:

            monitors = [
                address for address, status in self._node_statuses.items() if status in self._healthy_statuses
            ]

            active_nodes_count = len(monitors)

            if active_nodes_count > 0:

                self._tasks_queue.extend([
                    asyncio.create_task(
                        self._push_state_to_node_tcp(
                            host=host,
                            port=port
                        )
                    ) for host, port in monitors
                ])

            await asyncio.sleep(
                self._sync_interval
            )

    async def _push_state_to_node(
        self,
        host: str,
        port: int
    ):
        
        updates = [
            self._push_status_update(
                host=host,
                port=port,
                target_host=node_host,
                target_port=node_port
            ) for node_host, node_port in self._node_statuses if self._node_statuses.get((
                node_host,
                node_port
            )) == 'healthy' and host != node_host and port != node_port
        ]

        if len(updates) > 0:
            await asyncio.gather(*updates)

    async def _push_state_to_node_tcp(
        self,
        host: str,
        port: int
    ):
        updates = [
            self._push_tcp_status_update(
                host=host,
                port=port,
                target_host=node_host,
                target_port=node_port
            ) for node_host, node_port in self._node_statuses if self._node_statuses.get((
                node_host,
                node_port
            )) == 'healthy' and host != node_host and port != node_port
        ]
    
        if len(updates) > 0:
            await asyncio.gather(*updates)

    async def _push_status_update(
        self,
        host: str,
        port: int,
        target_host: Optional[str]=None,
        target_port: Optional[int]=None
    ) -> Tuple[
            Union[int, None], 
            Union[HealthCheck, None]
        ]:
        
        shard_id: Union[int, None] = None
        healthcheck: Union[HealthCheck, None] = None

        for _ in range(self._poll_retries):

            try:

                response: Tuple[int, HealthCheck] = await asyncio.wait_for(
                    self.push_status_update(
                        host,
                        port,
                        self.status,
                        target_host=target_host,
                        target_port=target_port,
                        error_context=self.error_context
                    ),
                    timeout=self._poll_timeout * (self._local_health_multiplier + 1)
                )

                shard_id, healthcheck = response
                source_host, source_port = healthcheck.source_host, healthcheck.source_port

                self._node_statuses[(source_host, source_port)] = healthcheck.status

                return shard_id, healthcheck

            except asyncio.TimeoutError:
                pass

        return shard_id, healthcheck

    async def _push_tcp_status_update(
        self,
        host: str,
        port: int,
        target_host: Optional[str]=None,
        target_port: Optional[int]=None
    ):
        shard_id: Union[int, None] = None
        healthcheck: Union[HealthCheck, None] = None
        
        try:
            
            response: Tuple[int, HealthCheck] = await asyncio.wait_for(
                self.push_tcp_status_update(
                    host,
                    port,
                    self.status,
                    target_host=target_host,
                    target_port=target_port,
                    error_context=self.error_context
                ),
                timeout=self._poll_timeout * (self._local_health_multiplier + 1)
            )

            shard_id, healthcheck = response
            source_host, source_port = healthcheck.source_host, healthcheck.source_port

            self._node_statuses[(source_host, source_port)] = healthcheck.status

            return shard_id, healthcheck

        except asyncio.TimeoutError:
            pass

        return shard_id, healthcheck


    async def _push_suspect_update(
        self,
        host: str,
        port: int,
        health_status: HealthStatus,
        error_context: Optional[str]=None
    ):
        
        try:
            response: Tuple[int, HealthCheck] = await asyncio.wait_for(
                self.push_suspect_update(
                    host=host,
                    port=port,
                    health_status=health_status,
                    error_context=error_context
                ),
                timeout=self._poll_timeout * (self._local_health_multiplier + 1)
            )

            _, healthcheck = response

            self._node_statuses[(host, port)] = healthcheck.status

        except asyncio.TimeoutError:
            pass

    async def cleanup_pending_checks(self):

        while self._running:

            for pending_check in list(self._tasks_queue):
                if pending_check.done() or pending_check.cancelled():
                    try:
                        await pending_check

                    except (
                        ConnectionRefusedError,
                        ConnectionAbortedError,
                        ConnectionResetError
                    ):
                        pass

                    self._tasks_queue.remove(pending_check)

            await asyncio.sleep(self._cleanup_interval)
    
    async def shutdown(self):
        self._running = False
        self._local_health_monitor.cancel()

        await asyncio.gather(*[
            cancel(check) for check in self._tasks_queue
        ])

        await asyncio.gather(*[
            cancel(remote_check) for remote_check in self._healthchecks.values()
        ])

        await cancel(self._local_health_monitor)
        
        await cancel(self._cleanup_task)

        await cancel(self._udp_sync_task)

        await cancel(self._tcp_sync_task)

        await self.close()

    async def soft_shutdown(self):
        await asyncio.gather(*[
            cancel(check) for check in self._tasks_queue
        ])


            
            


    


