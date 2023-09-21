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
from hedra.distributed.models.raft import HealthCheck, HealthStatus
from hedra.distributed.service.controller import Controller
from hedra.distributed.snowflake import Snowflake
from hedra.distributed.types import Call
from hedra.logging import (
    HedraLogger,
    logging_manager
)
from hedra.tools.helpers import cancel
from typing import Optional, Dict, Tuple, List, Deque, Union


class Monitor(Controller):

    def __init__(
        self,
        host: str,
        port: int,
        env: Optional[Env]=None,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None,
        logs_directory: Optional[str]=None,
        workers: int=0,
    ) -> None:
        
        if workers <= 1:
            engine = 'async'

        else:
            engine = 'process'

        if env is None:
            env: Env = load_env(Env)

        if logs_directory is None:
            logs_directory = env.MERCURY_SYNC_LOGS_DIRECTORY
            
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
        
        self._healthcheck_task: Union[asyncio.Task, None] = None
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

        self._local_health_multipliers: Dict[
            Tuple[str, int],
            float
        ] = defaultdict(lambda: 0)

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

        self._suspect_max_age= TimeParser(
            monitor_env.MERCURY_SYNC_SUSPECT_MAX_AGE
        ).time

        self._check_nodes_count = monitor_env.MERCURY_SYNC_INDIRECT_CHECK_NODES

        self.min_suspect_multiplier = monitor_env.MERCURY_SYNC_MIN_SUSPECT_TIMEOUT_MULTIPLIER
        self.max_suspect_multiplier = monitor_env.MERCURY_SYNC_MAX_SUSPECT_TIMEOUT_MULTIPLIER 
        self._min_suspect_node_count = monitor_env.MERCURY_SYNC_MIN_SUSPECT_NODES_THRESHOLD
        self._max_poll_multiplier = monitor_env.MERCURY_SYNC_MAX_POLL_MULTIPLIER
        self._initial_expected_nodes = monitor_env.MERCURY_SYNC_EXPECTED_NODES

        self._confirmed_suspicions: Dict[Tuple[str, int], int] = defaultdict(lambda: 0)
        self._registered_counts: Dict[Tuple[str, int], int] = defaultdict(lambda: 0)
        self._waiter: Union[asyncio.Future, None] = None

        self._tasks_queue: Deque[asyncio.Task] = deque()
        self._degraded_nodes: Deque[Tuple[str, int]] = deque()
        self._suspect_nodes: Deque[Tuple[str, int]] = deque()
        self._suspect_history: List[Tuple[str, int, int]] = []

        self._degraded_tasks: Dict[Tuple[str, int], asyncio.Task] = {}
        self._suspect_tasks: Dict[Tuple[str, int], asyncio.Task] = {}
        self._latest_update: Dict[Tuple[str,int], int] = {}

        self._local_health_monitor: Union[asyncio.Task, None] = None
        self._udp_sync_task: Union[asyncio.Task, None] = None
        self._tcp_sync_task: Union[asyncio.Task, None] = None

        self._cleanup_task: Union[asyncio.Task, None] = None
        self._investigating_nodes: Dict[Tuple[str, int], Dict[Tuple[str, int]]] = defaultdict(dict)
        self._node_statuses: Dict[Tuple[str, int], HealthStatus] = {}
        self._instance_ids: Dict[Tuple[str, int], int] = {}

        self._models = [
            HealthCheck
        ]

        self.bootstrap_host: Union[str, None] = None
        self.bootstrap_port: Union[int, None] = None
        
        logging_manager.logfiles_directory = logs_directory
        logging_manager.update_log_level(
            env.MERCURY_SYNC_LOG_LEVEL
        )

        self._logger = HedraLogger()
        self._logger.initialize()

        self._healthy_statuses = [
            'initializing',
            'waiting',
            'healthy'
        ]

        self._unhealthy_statuses = [
            'suspect',
            'failed'
        ]

        self.failed_nodes: List[Tuple[str, int, float]] = []
        self.removed_nodes: List[Tuple[str, int, float]] = []

        self._failed_max_age = TimeParser(
            monitor_env.MERCURY_SYNC_FAILED_NODES_MAX_AGE
        ).time

        self._removed_max_age = TimeParser(
            monitor_env.MERCURY_SYNC_REMOVED_NODES_MAX_AGE
        ).time

    @server()
    async def register_node(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        
        try:
            source_host = healthcheck.source_host
            source_port = healthcheck.source_port

            not_self = self._check_is_not_self(
                source_host,
                source_port
            )

            not_registered = self._check_is_not_registered(
                source_host,
                source_port
            )

            if not_self and not_registered: 
                self._node_statuses[(source_host, source_port)] = 'healthy'

            snowflake = Snowflake.parse(shard_id)
            self._instance_ids[(source_host, source_port)] = snowflake.instance 

            if healthcheck.registered_nodes:

                for host, port, instance_id in healthcheck.registered_nodes:

                    not_self = self._check_is_not_self(
                        host,
                        port
                    )

                    not_registered = self._check_is_not_registered(
                        host,
                        port
                    )

                    if not_self and not_registered:
                        self._node_statuses[(host, port)] = 'healthy'

                        self._tasks_queue.append(
                            asyncio.create_task(
                                self._cancel_suspicion_probe(
                                    host,
                                    port
                                )
                            )
                        )
                    
                    self._instance_ids[(host, port)] = instance_id

            node_address = (source_host, source_port)

            self._tasks_queue.append(
                asyncio.create_task(
                    self._cancel_suspicion_probe(
                        source_host,
                        source_port
                    )
                )
            )

            if node_address in self.failed_nodes:
                self.failed_nodes.remove(node_address)

            self._registered_counts[(source_host, source_port)] = max(
                healthcheck.registered_count,
                self._registered_counts[(source_host, source_port)]
            )

            return HealthCheck(
                host=source_host,
                port=source_port,
                source_host=self.host,
                source_port=self.port,
                registered_nodes=[
                    (
                        host,
                        port,
                        self._instance_ids.get((
                            host, port
                        ))
                    ) for host, port in self._instance_ids
                ],
                status=self.status,
                registered_count=len(self._instance_ids)
            )

        except Exception:
            pass

    @server()
    async def deregister_node(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        
        source_host = healthcheck.source_host
        source_port = healthcheck.source_port

        node = self._node_statuses.get((
            source_host,
            source_port
        ))

        await self._logger.distributed.aio.info(f'Node - {source_host}:{source_port} - submitted request to leave to source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Node - {source_host}:{source_port} - submitted request to leave to source - {self.host}:{self.port}')

        if self._suspect_tasks.get((
            source_host,
            source_port
        )):

            self._tasks_queue.append(
                asyncio.create_task(
                    self._cancel_suspicion_probe(
                        source_host,
                        source_port
                    )
                )
            )
            
            await self._logger.distributed.aio.debug(f'Source - {self.host}:{self.port} - has cancelled suspicion of node - {source_host}:{source_port} - due to leave request')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - has cancelled suspicion of node - {source_host}:{source_port} - due to leave request')

        if node is not None:
            
            node_status = "inactive"
            self._node_statuses[(source_host, source_port)] = node_status
            
            await self._logger.distributed.aio.debug(f'Source - {self.host}:{self.port} - has accepted request to remove node - {source_host}:{source_port}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - has accepted request to remove node - {source_host}:{source_port}')


        return HealthCheck(
            host=healthcheck.source_host,
            port=healthcheck.source_port,
            source_host=self.host,
            source_port=self.port,
            status=self.status
        )

    @server()
    async def update_node_status(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
        
        update_node_host = healthcheck.source_host
        update_node_port = healthcheck.source_port
        update_status = healthcheck.status

        await self._logger.distributed.aio.debug(f'Node - {update_node_host}:{update_node_port} - updating status to - {update_status} - for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Node - {update_node_host}:{update_node_port} - updating status to - {update_status} - for source - {self.host}:{self.port}')

        if healthcheck.target_host and healthcheck.target_port:
            update_node_host = healthcheck.target_host
            update_node_port = healthcheck.target_port

        if healthcheck.target_status:
            update_status = healthcheck.target_status

        target_last_updated: Union[int, None] = healthcheck.target_last_updated
        local_last_updated: Union[int, None] = self._latest_update.get((
            update_node_host,
            update_node_port
        ), 0)

        snowflake = Snowflake.parse(shard_id)

        source_host = healthcheck.source_host
        source_port = healthcheck.source_port
        self._instance_ids[(source_host, source_port)] = snowflake.instance

        if target_last_updated > local_last_updated:
            self._node_statuses[(update_node_host, update_node_port)] = update_status

        self._local_health_multipliers[(update_node_host, update_node_port)] = self._reduce_health_multiplier(
            update_node_host,
            update_node_port
        )
        
        await self._logger.distributed.aio.debug(f'Node - {update_node_host}:{update_node_port} - updated status to - {update_status} - for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Node - {update_node_host}:{update_node_port} - updated status to - {update_status} - for source - {self.host}:{self.port}')

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

        await self._logger.distributed.aio.debug(f'Node - {source_host}:{source_port} - requested a check for suspect source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Node - {source_host}:{source_port} - requested a check for suspect source - {self.host}:{self.port}')

        if self.status == 'healthy':

            await self._logger.distributed.aio.debug(f'Source - {self.host}:{self.port} - received notification it is suspect despite being healthy from node - {source_host}:{source_port}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Source - {self.host}:{self.port} - received notification it is suspect despite being healthy from node - {source_host}:{source_port}')

            self._local_health_multipliers[(source_host, source_port)] = self._increase_health_multiplier(
                source_host,
                source_port
            )

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

        await self._logger.distributed.aio.debug(f'Node - {source_host}:{source_port} - requested an indirect check for node - {target_host}:{target_port} - from source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Node - {source_host}:{source_port} - requested an indirect check for node - {target_host}:{target_port} - from source - {self.host}:{self.port}')

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

                self._local_health_multipliers[(target_host, target_port)] = self._reduce_health_multiplier(
                    target_host,
                    target_port
                )
                
            await self._logger.distributed.aio.debug(f'Suspect node - {target_host}:{target_port} - responded to an indirect check from source - {self.host}:{self.port} - for node - {source_host}:{source_port}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Suspect node - {target_host}:{target_port} - responded to an indirect check from source - {self.host}:{self.port} - for node - {source_host}:{source_port}')

        except Exception:

            if self._node_statuses[(target_host, target_port)] != 'failed':

                await self._logger.distributed.aio.debug(f'Suspect node - {target_host}:{target_port} - failed to respond to an indirect check from source - {self.host}:{self.port} - for node - {source_host}:{source_port}')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Suspect node - {target_host}:{target_port} - failed to respond to an indirect check from source - {self.host}:{self.port} - for node - {source_host}:{source_port}')

                self._local_health_multipliers[(target_host, target_port)] = self._increase_health_multiplier(
                    target_host,
                    target_port
                )

                # Our suspicion is correct!
                return HealthCheck(
                    host=healthcheck.source_host,
                    port=healthcheck.source_port,
                    source_host=target_host,
                    source_port=target_port,
                    target_status='suspect', 
                    status=self.status
                )
            
        return HealthCheck(
            host=healthcheck.source_host,
            port=healthcheck.source_port,
            target_status=self._node_statuses.get((target_host, target_port)),   
            source_host=target_host,
            source_port=target_port,
            status=self.status,
            error=self.error_context
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


        await self._logger.distributed.aio.debug(f'Node - {source_host}:{source_port} - acknowledged the indirect check request for node - {target_host}:{target_port} - for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Node - {source_host}:{source_port} - acknowledged the indirect check request for node - {target_host}:{target_port} - for source - {self.host}:{self.port}')

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
    async def update_node_health(
        self,
        shard_id: int,
        healthcheck: HealthCheck
    ) -> Call[HealthCheck]:
            
        try:
            update_node_host = healthcheck.source_host
            update_node_port = healthcheck.source_port

            local_node_status = self._node_statuses.get((update_node_host, update_node_port))

            if self._suspect_tasks.get((
                update_node_host,
                update_node_port
            )):

                await self._logger.distributed.aio.debug(f'Node - {update_node_host}:{update_node_port} - submitted healthy status to source - {self.host}:{self.port} - and is no longer suspect')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Node - {update_node_host}:{update_node_port} - submitted healthy status to source - {self.host}:{self.port} - and is no longer suspect')

                self._tasks_queue.append(
                    asyncio.create_task(
                        self._cancel_suspicion_probe(
                            update_node_host,
                            update_node_port
                        )
                    )
                )

            snowflake = Snowflake.parse(shard_id)
            
            self._node_statuses[(update_node_host, update_node_port)] = healthcheck.status
            self._latest_update[(update_node_host, update_node_port)] = snowflake.timestamp

            return HealthCheck(
                host=healthcheck.source_host,
                port=healthcheck.source_port,
                source_host=self.host,
                source_port=self.port,
                source_status=local_node_status,
                error=self.error_context,
                status=self.status
            )
        
        except Exception:
            return HealthCheck(
                host=healthcheck.source_host,
                port=healthcheck.source_port,
                source_host=self.host,
                source_port=self.port,
                source_status=local_node_status,
                error=self.error_context,
                status=self.status
            )
    
    @client('register_node')
    async def submit_registration(
        self,
        host: str,
        port: int
    ) -> Call[HealthCheck]:    
        return HealthCheck(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            registered_nodes=[
                (
                    host,
                    port,
                    self._instance_ids.get((
                        host,
                        port
                    ))
                ) for host, port in self._instance_ids
            ],
            registered_count=len(self._instance_ids),
            error=self.error_context,
            status=self.status
        )

    @client('update_node_health')
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
    
    @client('update_node_health', as_tcp=True)
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


    
    async def _cancel_suspicion_probe(
        self,
        suspect_host: str,
        suspect_port: int
    ):
        
        suspect_node = (suspect_host, suspect_port)

        suspect_tasks = dict(self._suspect_tasks)
        suspect_task = suspect_tasks.get(suspect_node)

        if suspect_task is not None:
            await cancel(suspect_task)
            del suspect_tasks[suspect_node]
            
        self._suspect_tasks = suspect_tasks

    async def _run_tcp_healthcheck(
        self, 
        host: str, 
        port: int,
        target_host: Optional[str]=None,
        target_port: Optional[str]=None
    ) -> Union[Tuple[int, HealthCheck], None]:
        
        shard_id: Union[int, None] = None
        healthcheck: Union[HealthCheck, None] = None

        await self._logger.distributed.aio.debug(f'Running TCP healthcheck for node - {host}:{port} - for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Running TCP healthcheck for node - {host}:{port} - for source - {self.host}:{self.port}')

        for idx in range(self._poll_retries):

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
                    timeout=self._calculate_current_timeout(
                        host,
                        port
                    )
                )

                shard_id, healthcheck = response
                source_host, source_port = healthcheck.source_host, healthcheck.source_port

                self._node_statuses[(source_host, source_port)] = healthcheck.status

                self._local_health_multipliers[(host, port)] = self._reduce_health_multiplier(
                    host,
                    port
                )

                return shard_id, healthcheck

            except Exception:
                self._local_health_multipliers[(host, port)] = self._increase_health_multiplier(
                    host,
                    port
                )
                
        check_host = host
        check_port = port

        if target_host and target_port:
            check_host = target_host
            check_port = target_port

        node_status = self._node_statuses.get((
            check_host, 
            check_port
        )) 

        not_self = self._check_is_not_self(
            check_host,
            check_port
        )

        if not_self and healthcheck is None and node_status == 'healthy':

            await self._logger.distributed.aio.debug(f'Node - {check_host}:{check_port} - failed to respond over - {self._poll_retries} - retries and is now suspect for source - {self.host}:{self.port}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Node - {check_host}:{check_port} - failed to respond over - {self._poll_retries} - retries and is now suspect for source - {self.host}:{self.port}')

            self._node_statuses[(check_host, check_port)] = 'suspect'

            self._suspect_nodes.append((
                check_host,
                check_port
            ))

            self._suspect_tasks[(host, port)] = asyncio.create_task(
                self._start_suspect_monitor()
            )

        else:
            await self._logger.distributed.aio.debug(f'Node - {check_host}:{check_port} - responded on try - {idx}/{self._poll_retries} - for source - {self.host}:{self.port}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Node - {check_host}:{check_port} - responded on try - {idx}/{self._poll_retries} - for source - {self.host}:{self.port}')

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
    
    @client('deregister_node')
    async def request_deregistration(
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

        await self._logger.filesystem.aio.create_logfile(f'hedra.distributed.{self._instance_id}.log')
        self._logger.filesystem.create_filelogger(f'hedra.distributed.{self._instance_id}.log')

        await self.start_server()

        boot_wait = random.uniform(0.1, self.boot_wait * self._initial_expected_nodes)
        await asyncio.sleep(boot_wait)

    async def register(
        self,
        host: str,
        port: int
    ):
    
        await self._logger.distributed.aio.info(f'Initializing node - {self.host}:{self.port} - with id - {self._instance_id}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Initializing node - {self.host}:{self.port} - with id - {self._instance_id}')
  
        self.bootstrap_host = host
        self.bootstrap_port = port
        self.status = 'healthy'

        await self._logger.distributed.aio.info(f'Connecting to node node - {self.bootstrap_host}:{self.bootstrap_port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Connecting to node node - {self.bootstrap_host}:{self.bootstrap_port}')
        
        await self._register_initial_node()
        
        self._running = True
        
        
        self._healthcheck_task = asyncio.create_task(
            self.start_health_monitor()
        )
        
        self._cleanup_task = asyncio.create_task(
            self.cleanup_pending_checks()
        )

        self._udp_sync_task = asyncio.create_task(
            self._run_udp_state_sync()
        )

        self._tcp_sync_task = asyncio.create_task(
            self._run_tcp_state_sync()
        )

        await self._logger.distributed.aio.info(f'Initialized node - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Initialized node - {self.host}:{self.port}')


        self.status = 'healthy'

    async def _register_initial_node(self):
        await self._logger.distributed.aio.info(f'Connecting to initial node - {self.bootstrap_host}:{self.bootstrap_port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Connecting to initial node - {self.bootstrap_host}:{self.bootstrap_port}')

        poll_timeout = self._poll_timeout * self._initial_expected_nodes

        try:

            self._node_statuses[(self.bootstrap_host, self.bootstrap_port)] = 'healthy'
            
            await asyncio.wait_for(
                self.start_client(
                    {
                        (self.bootstrap_host, self.bootstrap_port): self._models
                    },
                    cert_path=self.cert_path,
                    key_path=self.key_path
                ),
                timeout=poll_timeout
            )

            while len(self._node_statuses) < 1:

                try:

                    shard_id, response = await asyncio.wait_for(
                        self.submit_registration(
                            self.bootstrap_host,
                            self.bootstrap_port
                        ),
                        timeout=poll_timeout
                    )

                    source_host = response.source_host
                    source_port = response.source_port

                    self._instance_ids[(source_host, source_port)] = Snowflake.parse(shard_id).instance

                except Exception:
                    pass

                await asyncio.sleep(self._poll_interval)

        except Exception:
            pass

    def _calculate_min_suspect_timeout(
        self,
        suspect_node_address: Tuple[str, int]
    ):
        nodes_count = len(self._node_statuses) + 1

        suspect_host, suspect_port = suspect_node_address

        poll_timeout = self._calculate_current_timeout(
            suspect_host,
            suspect_port
        )

        return round(
            self.min_suspect_multiplier * math.log10(nodes_count) * poll_timeout,
            2
        )
    
    def _reduce_health_multiplier(
        self,
        host: str,
        port: int
    ) -> int:
        
        modifier = len([
            address for address, status in self._node_statuses.items() if status == 'healthy' 
        ])

        return max(
            self._local_health_multipliers[(host, port)] - (1 * modifier),
            0
        )
    
    def _increase_health_multiplier(
        self,
        host: str,
        port: int
    ) -> int:
        
        return min(
            self._local_health_multipliers[(host, port)] + 1,
            self.max_suspect_multiplier
        )
    
    def _calculate_current_timeout(
        self,
        host: str,
        port: int
    ):
        modifier = max(
            len([
                address for address, status in self._node_statuses.items() if status == 'healthy' 
            ]),
            self._initial_expected_nodes
        )

        return self._poll_timeout + (self._local_health_multipliers[(host, port)] + 1) * modifier
    
    def _calculate_current_poll_interval(
        self,
        host: str,
        port: int
    ) -> float:
        return self._poll_interval * (self._local_health_multipliers[(host, port)] + 1)

    def _calculate_max_suspect_timeout(self, min_suspect_timeout: float):
        
        return round(
            self.max_suspect_multiplier * min_suspect_timeout,
            2
        )

    def _calculate_suspicion_timeout(
        self,
        suspect_node_address: Tuple[str, int]
    ):

        min_suspect_timeout = self._calculate_min_suspect_timeout(suspect_node_address)
        
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
    
    def _check_is_not_self(
        self,
        host: str,
        port: int
    ):
        return host != self.host and port != self.port
    
    def _check_is_not_registered(
        self,
        host: str,
        port: int
    ):
        return self._node_statuses.get((
            host,
            port
        )) is None
    
    async def _acknowledge_indirect_probe(
        self,
        host: str,
        port: int,
        target_host: str,
        target_port: int
    ):
        shard_id: Union[int, None] = None
        healthcheck: Union[HealthCheck, None] = None
        
        await self._logger.distributed.aio.debug(f'Running UDP healthcheck for node - {host}:{port} - for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Running UDP healthcheck for node - {host}:{port} - for source - {self.host}:{self.port}')

        for idx in range(self._poll_retries):

            try:

                await self._logger.distributed.aio.debug(f'Sending indirect check request to - {target_host}:{target_port} -for node - {host}:{port} - from source - {self.host}:{self.port}')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Sending indirect check request to - {target_host}:{target_port} -for node - {host}:{port} - from source - {self.host}:{self.port}')

                response: Tuple[int, HealthCheck] = await asyncio.wait_for(
                    self.push_acknowledge_check(
                        host,
                        port,
                        target_host,
                        target_port,
                        self.status,
                        error_context=self.error_context
                    ),
                    timeout=self._calculate_current_timeout(
                        host,
                        port
                    )
                )

                shard_id, healthcheck = response

                source_host, source_port = healthcheck.source_host, healthcheck.source_port

                not_self = self._check_is_not_self(
                    source_host,
                    source_port
                )

                if not_self:
                    self._node_statuses[(source_host, source_port)] = healthcheck.status

                await self._logger.distributed.aio.debug(f'Completed indirect check request to - {target_host}:{target_port} -for node - {host}:{port} - from source - {self.host}:{self.port} - on try - {idx}/{self._poll_retries}')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Completed indirect check request to - {target_host}:{target_port} -for node - {host}:{port} - from source - {self.host}:{self.port} - on try - {idx}/{self._poll_retries}')

                return shard_id, healthcheck

            except Exception:
                pass              

    async def _run_healthcheck(
        self, 
        host: str, 
        port: int,
        target_host: Optional[str]=None,
        target_port: Optional[str]=None
    ) -> Union[Tuple[int, HealthCheck], None]:
        
        shard_id: Union[int, None] = None
        healthcheck: Union[HealthCheck, None] = None
        
        await self._logger.distributed.aio.debug(f'Running UDP healthcheck for node - {host}:{port} - for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Running UDP healthcheck for node - {host}:{port} - for source - {self.host}:{self.port}')

        for idx in range(self._poll_retries):

            timeout = self._calculate_current_timeout(
                host,
                port
            )

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
                    timeout=timeout
                )

                shard_id, healthcheck = response
                source_host, source_port = healthcheck.source_host, healthcheck.source_port

                not_self = self._check_is_not_self(
                    source_host,
                    source_port
                )

                if not_self:
                    self._node_statuses[(source_host, source_port)] = healthcheck.status

                self._local_health_multipliers[(host, port)] = self._reduce_health_multiplier(
                    host,
                    port
                )

                await self._logger.distributed.aio.debug(f'Node - {host}:{port} - responded on try - {idx}/{self._poll_retries} - for source - {self.host}:{self.port}')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Node - {host}:{port} - responded on try - {idx}/{self._poll_retries} - for source - {self.host}:{self.port}')

                return shard_id, healthcheck

            except Exception:

                await self._logger.distributed.aio.debug(f'Node - {host}:{port} - failed for source node - {self.host}:{self.port} - on attempt - {idx}/{self._poll_retries}')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Node - {host}:{port} - failed for source node - {self.host}:{self.port} - on attempt - {idx}/{self._poll_retries}')

                self._local_health_multipliers[(host, port)] = self._increase_health_multiplier(
                    host,
                    port
                )

        check_host = host
        check_port = port

        if target_host and target_port:
            check_host = target_host
            check_port = target_port

        node_status = self._node_statuses.get((
            check_host, 
            check_port
        )) 

        not_self = self._check_is_not_self(
            check_host,
            check_port
        )

        if not_self and healthcheck is None and node_status== 'healthy':

            await self._logger.distributed.aio.debug(f'Node - {check_host}:{check_port} - failed to respond over - {self._poll_retries} - retries and is now suspect for source - {self.host}:{self.port}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Node - {check_host}:{check_port} - failed to respond over - {self._poll_retries} - retries and is now suspect for source - {self.host}:{self.port}')

            self._node_statuses[(check_host, check_port)] = 'suspect'

            self._suspect_nodes.append((
                check_host,
                check_port
            ))

            self._suspect_tasks[(host, port)] = asyncio.create_task(
                self._start_suspect_monitor()
            )
        
        return shard_id, healthcheck

    async def _start_suspect_monitor(self) -> Tuple[str, int]:

        if len(self._suspect_nodes) < 1:
            return
        
        address = self._suspect_nodes.pop() 
        suspect_host, suspect_port = address


        not_self = self._check_is_not_self(
            suspect_host,
            suspect_port
        )

        if not_self and address not in self._suspect_history:
            self._suspect_history.append((
                suspect_host,
                suspect_port,
                time.monotonic()
            ))

        else:
            return


        status = self._node_statuses[(suspect_host, suspect_port)] 

        if status == 'suspect':

            await self._logger.distributed.aio.debug(f'Node - {suspect_host}:{suspect_port} - marked suspect for source {self.host}:{self.port}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Node - {suspect_host}:{suspect_port} - marked suspect for source {self.host}:{self.port}')
            
        suspicion_timeout = self._calculate_suspicion_timeout(address)

        elapsed = 0
        start = time.monotonic()

        while elapsed < suspicion_timeout and status == 'suspect':

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

            await self._logger.distributed.aio.debug(f'Source - {self.host}:{self.port} - acknowledged - {indirect_ack_count} - indirect probes and failed to acknowledge - {missing_ack_count} - indirect probes.')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - acknowledged - {indirect_ack_count} - indirect probes and failed to acknowledge - {missing_ack_count} - indirect probes.')

            next_health_multiplier = self._local_health_multipliers[(suspect_host, suspect_port)] + missing_ack_count - indirect_ack_count
            if next_health_multiplier < 0:
                self._local_health_multipliers[(suspect_host, suspect_port)] = 0

            else:
                self._local_health_multipliers[(suspect_host, suspect_port)] = self._increase_health_multiplier(
                    suspect_host,
                    suspect_port
                )

            confirmation_members_count = len(confirmation_members)

            if suspect_count < confirmation_members_count:
                # We had a majority confirmation the node was healthy.
                self._investigating_nodes[(suspect_host, suspect_port)] = {}
                self._confirmed_suspicions[(suspect_host, suspect_port)] = 0

                self._node_statuses[(suspect_host, suspect_port)] = 'healthy'

                self._reduce_health_multiplier(
                    suspect_host,
                    suspect_port
                )

                await self._logger.distributed.aio.info(f'Node - {suspect_host}:{suspect_port} - successfully responded to one or more probes for source - {self.host}:{self.port}')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Node - {suspect_host}:{suspect_port} - failed to respond for source - {self.host}:{self.port}. Setting next timeout as - {suspicion_timeout}')

                break
            
            await asyncio.sleep(
                self._calculate_current_poll_interval(
                    suspect_host,
                    suspect_port
                )
            )

            
            status = self._node_statuses[(suspect_host, suspect_port)] 

            elapsed = time.monotonic() - start
            suspicion_timeout = self._calculate_suspicion_timeout(address)


            await self._logger.distributed.aio.debug(f'Node - {suspect_host}:{suspect_port} - failed to respond for source - {self.host}:{self.port}. Setting next timeout as - {suspicion_timeout}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Node - {suspect_host}:{suspect_port} - failed to respond for source - {self.host}:{self.port}. Setting next timeout as - {suspicion_timeout}')

        if self._node_statuses[(suspect_host, suspect_port)] == 'suspect':
            self._node_statuses[(suspect_host, suspect_port)] = 'failed'

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

            await self._logger.distributed.aio.info(f'Node - {suspect_host}:{suspect_port} - marked failed for source - {self.host}:{self.port}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Node - {suspect_host}:{suspect_port} - marked failed for source - {self.host}:{self.port}')
        
        self._investigating_nodes[(suspect_host, suspect_port)] = {}
        self._confirmed_suspicions[(suspect_host, suspect_port)] = 0

        return (
            suspect_host,
            suspect_port
        )
        
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
        
        await self._logger.distributed.aio.debug(f'Requesting indirect check for node -  {host}:{port} - for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Requesting indirect check for node -  {host}:{port} - for source - {self.host}:{self.port}')

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
            timeout=self._calculate_current_timeout(
                host,
                port
            )
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
            ) and isinstance(
                result[0], 
                int
            ) and isinstance(
                result[1],
                HealthCheck
            )
        ]

        errors = [
            result for result in results if result not in healthchecks
        ]

        sorted_checks: List[Call[HealthCheck]] = list(sorted(
            healthchecks,
            key=lambda check: Snowflake.parse(check[0]).timestamp
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
        ], return_exceptions=True)


        await self._logger.distributed.aio.debug(f'Total of {suspect_count} nodes confirmed node - {host}:{port} - is suspect for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Total of {suspect_count} nodes confirmed node -  {host}:{port} - is suspect for source - {self.host}:{self.port}')

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

            node_status = self._node_statuses.get((host, port))
            if node_status in self._healthy_statuses:
        
                self._tasks_queue.append(
                    asyncio.create_task(
                        self._run_healthcheck(
                            host,
                            port
                        )
                    )
                )

            await asyncio.sleep(
                self._calculate_current_poll_interval(
                    host,
                    port
                )
            )

    async def leave(self):

        await self._submit_leave_requests()
        await self._shutdown()

    async def _submit_leave_requests(self):
        monitors = [
            address for address, status in self._node_statuses.items() if status in self._healthy_statuses
        ]

        if len(monitors) > 0:
            await asyncio.gather(*[ 
                asyncio.create_task(
                    self.request_deregistration(
                        host,
                        port,
                        self.status,
                        error_context=self.error_context
                    )
                ) for host, port in monitors
            ])

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
            asyncio.create_task(
                self._push_tcp_status_update(
                    host=host,
                    port=port,
                    target_host=node_host,
                    target_port=node_port
                )
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

        await self._logger.distributed.aio.debug(f'Pushing UDP health update for source - {host}:{port} - to node - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Pushing UDP health update for source - {host}:{port} - to node - {self.host}:{self.port}')

        for _ in range(self._poll_retries):

            try:

                timeout = self._calculate_current_timeout(
                    host,
                    port
                )

                response: Tuple[int, HealthCheck] = await asyncio.wait_for(
                    self.push_status_update(
                        host,
                        port,
                        self.status,
                        target_host=target_host,
                        target_port=target_port,
                        error_context=self.error_context
                    ),
                    timeout=timeout
                )

                shard_id, healthcheck = response
                source_host, source_port = healthcheck.source_host, healthcheck.source_port

                not_self = self._check_is_not_self(
                    source_host,
                    source_port
                )
                
                if not_self:
                    self._node_statuses[(source_host, source_port)] = healthcheck.status

                return shard_id, healthcheck

            except Exception:

                self._local_health_multipliers[(host, port)] = self._increase_health_multiplier(
                    host,
                    port
                )

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

        await self._logger.distributed.aio.debug(f'Pushing TCP health update for source - {host}:{port} - to node - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Pushing TCP health update for source - {host}:{port} - to node - {self.host}:{self.port}')
        
        for _ in range(self._poll_retries):
            
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
                    timeout=self._calculate_current_timeout(
                        host,
                        port
                    )
                )

                self._local_health_multipliers[(host, port)] = self._reduce_health_multiplier(
                    host,
                    port
                )
                shard_id, healthcheck = response
                source_host, source_port = healthcheck.source_host, healthcheck.source_port

                not_self = self._check_is_not_self(
                    source_host,
                    source_port
                )

                if not_self:
                    self._node_statuses[(source_host, source_port)] = healthcheck.status

                return shard_id, healthcheck

            except Exception:

                self._local_health_multipliers[(host, port)] = self._increase_health_multiplier(
                    host,
                    port
                )

        return shard_id, healthcheck


    async def _push_suspect_update(
        self,
        host: str,
        port: int,
        health_status: HealthStatus,
        error_context: Optional[str]=None
    ):
        
        await self._logger.distributed.aio.debug(f'Pushing TCP health update for source - {host}:{port} - to suspect node - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Pushing TCP health update for source - {host}:{port} - to suspect node - {self.host}:{self.port}')
        
        try:
            response: Tuple[int, HealthCheck] = await asyncio.wait_for(
                self.push_suspect_update(
                    host=host,
                    port=port,
                    health_status=health_status,
                    error_context=error_context
                ),
                timeout=self._calculate_current_timeout(
                    host,
                    port
                )
            )

            _, healthcheck = response

            not_self = self._check_is_not_self(
                host,
                port
            )

            if not_self:
                self._node_statuses[(host, port)] = healthcheck.status

        except Exception:
            pass          

    async def cleanup_pending_checks(self):

        await self._logger.distributed.aio.debug(f'Running cleanup for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Running cleanup for source - {self.host}:{self.port}')

        while self._running:

            pending_checks_count = 0

            for pending_check in list(self._tasks_queue):
                if pending_check.done() or pending_check.cancelled():
                    try:
                        await pending_check

                    except Exception:
                        pass

                    self._tasks_queue.remove(pending_check)
                    pending_checks_count += 1

            for node in list(self._suspect_history):
                _, _, age = node

                failed_elapsed = time.monotonic() - age

                if failed_elapsed >= self._suspect_max_age:
                    self._suspect_history.remove(node)


            for node in list(self.failed_nodes):

                _, _, age = node
                failed_elapsed = time.monotonic() - age
                removed_elapsed = time.monotonic() - age

                if node not in self.removed_nodes:
                    self.removed_nodes.append(node)

                if failed_elapsed >= self._failed_max_age:
                    self.failed_nodes.remove(node)

                elif removed_elapsed >= self._removed_max_age:
                    self.removed_nodes.remove(node)

            await self._logger.distributed.aio.debug(f'Cleaned up - {pending_checks_count} - for source - {self.host}:{self.port}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Cleaned up - {pending_checks_count} - for source - {self.host}:{self.port}')

            await asyncio.sleep(self._cleanup_interval)
    
    async def _shutdown(self):

        await self._logger.distributed.aio.debug(f'Shutdown requested for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Shutdown requested for source - {self.host}:{self.port}')

        self._running = False

        await asyncio.gather(*[
            cancel(check) for check in self._tasks_queue
        ], return_exceptions=True)

        if self._healthcheck_task:
            await cancel(self._healthcheck_task)

        if self._local_health_monitor:
            await cancel(self._local_health_monitor)
        
        if self._cleanup_task:
            await cancel(self._cleanup_task)
        
        if self._udp_sync_task:
            await cancel(self._udp_sync_task)

        if self._tcp_sync_task:
            await cancel(self._tcp_sync_task)

        await self.close()

        await self._logger.distributed.aio.debug(f'Shutdown complete for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Shutdown complete for source - {self.host}:{self.port}')

    async def soft_shutdown(self):
        await asyncio.gather(*[
            cancel(check) for check in self._tasks_queue
        ], return_exceptions=True)


            
            


    


