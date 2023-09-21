import asyncio
import random
import time
from collections import (
    defaultdict,
    deque
)
from hedra.distributed.env import (
    Env, 
    ReplicationEnv,
    load_env
)
from hedra.distributed.env.time_parser import TimeParser
from hedra.distributed.hooks.client_hook import client
from hedra.distributed.hooks.server_hook import server
from hedra.distributed.models.raft import (
    RaftMessage,
    HealthCheck,
    ElectionState,
    VoteResult
)
from hedra.distributed.types import Call
from hedra.distributed.models.raft.logs import Entry, NodeState
from hedra.distributed.monitoring import Monitor
from hedra.distributed.snowflake.snowflake_generator import (
    SnowflakeGenerator,
    Snowflake
)
from hedra.logging import (
    HedraLogger,
    logging_manager
)
from hedra.tools.helpers import cancel
from typing import (
    Optional, 
    Union, 
    Deque,
    Dict, 
    Tuple, 
    List,
    Any
)
from .log_queue import LogQueue


class ReplicationController(Monitor):

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
        
        if env is None:
            env = load_env(Env)

        if logs_directory is None:
            logs_directory = env.MERCURY_SYNC_LOGS_DIRECTORY

        replication_env = load_env(ReplicationEnv) 

        super().__init__(
            host,
            port,
            env=env,
            cert_path=cert_path,
            key_path=key_path,
            workers=workers,
            logs_directory=logs_directory
        )

        self._models = [
            HealthCheck,
            RaftMessage
        ]

        self._term_number = 0
        self._term_votes = defaultdict(
            lambda: defaultdict(
                lambda: 0
            )
        )

        self._max_election_timeout = TimeParser(
            replication_env.MERCURY_SYNC_RAFT_ELECTION_MAX_TIMEOUT
        ).time

        self._min_election_timeout = max(
            self._max_election_timeout * 0.5,
            1
        )

        self._election_poll_interval = TimeParser(
            replication_env.MERCURY_SYNC_RAFT_ELECTION_POLL_INTERVAL
        ).time

        self._logs_update_poll_interval = TimeParser(
            replication_env.MERCURY_SYNC_RAFT_LOGS_UPDATE_POLL_INTERVAL
        ).time
        
        self._election_status = ElectionState.READY
        self._raft_node_status = NodeState.FOLLOWER
        self._active_election_waiter: Union[asyncio.Future, None] = None
        self._latest_election: Dict[int, int] = {}
        self._term_leaders: List[Tuple[str, int]] = []
        
        self._running = False

        self._logs = LogQueue()
        self._previous_entry_index = 0
        self._term_number = 0

        self._raft_monitor_task: Union[asyncio.Task, None] = None
        self._tasks_queue: Deque[asyncio.Task] = deque()
        self._entry_id_generator = SnowflakeGenerator(self._instance_id)

        logging_manager.logfiles_directory = logs_directory
        logging_manager.update_log_level(
            env.MERCURY_SYNC_LOG_LEVEL
        )

        self._logger = HedraLogger()
        self._logger.initialize()


        self._election_poll_interval = TimeParser(
            replication_env.MERCURY_SYNC_RAFT_ELECTION_POLL_INTERVAL
        ).time

        self._cleanup_interval = TimeParser(
            env.MERCURY_SYNC_CLEANUP_INTERVAL
        ).time

        self.registration_timeout = TimeParser(
            replication_env.MERCURY_SYNC_RAFT_REGISTRATION_TIMEOUT
        ).time

        self._pending_election_waiter: Union[asyncio.Future, None]  = None

        self._election_timeout = random.uniform(
            self._min_election_timeout,
            self._max_election_timeout
        )

        self._raft_cleanup_task: Union[asyncio.Future, None] = None
        self._election_task: Union[asyncio.Task, None] = None 
        self._active_election = False
        
    async def start(self):

        await self._logger.filesystem.aio.create_logfile(f'hedra.distributed.{self._instance_id}.log')
        self._logger.filesystem.create_filelogger(f'hedra.distributed.{self._instance_id}.log')

        await self._logger.distributed.aio.info(f'Starting server for node - {self.host}:{self.port} - with id - {self._instance_id}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Starting server for node - {self.host}:{self.port} - with id - {self._instance_id}')

        await self.start_server()

        self._instance_ids[(self.host, self.port)] = Snowflake.parse(
            self._entry_id_generator.generate()
        ).instance

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

        await self._register_initial_node()
        await self._run_registration()

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

        boot_wait = random.uniform(0.1, self.boot_wait * self._initial_expected_nodes)
        await asyncio.sleep(boot_wait)

        if self._term_number == 0:
            self._election_status = ElectionState.ACTIVE
            await self.run_election()

        self._raft_cleanup_task = asyncio.create_task(
            self._cleanup_pending_raft_tasks()
        )

        self._raft_monitor_task = asyncio.create_task(
            self._run_raft_monitor()
        )

        self.status = 'healthy'

    async def _run_registration(self):

        last_registered_count = -1
        poll_timeout = self.registration_timeout * self._initial_expected_nodes
        
        while self._check_all_nodes_registered() is False:

            monitors = [
                address for address in self._node_statuses.keys()
            ]

            active_nodes_count = len(monitors)
            registered_count = self._calculate_all_registered_nodes()

            if registered_count > last_registered_count:
                await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - reporting - {registered_count}/{self._initial_expected_nodes} - as fully registered')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - reporting - {registered_count}/{self._initial_expected_nodes} - as fully registered')

                last_registered_count = registered_count

            if active_nodes_count > 0:

                for host, port in monitors:
                    self._tasks_queue.append(
                        asyncio.create_task(
                            asyncio.wait_for(
                                self._submit_registration(
                                    host,
                                    port
                                ),
                                timeout=poll_timeout
                            )
                        )
                    )

                    await asyncio.sleep(self._poll_interval)

            await asyncio.sleep(self._poll_interval)

        registered_count = self._calculate_all_registered_nodes()

        await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - reporting - {registered_count}/{self._initial_expected_nodes} - as fully registered')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - reporting - {registered_count}/{self._initial_expected_nodes} - as fully registered')

    def _calculate_all_registered_nodes(self) -> int:
        self._registered_counts[(self.host, self.port)] = len(self._instance_ids)
        return len([
            count for count in self._registered_counts.values() if count == self._initial_expected_nodes
        ])

    def _check_all_nodes_registered(self) -> bool:
        return self._calculate_all_registered_nodes() == self._initial_expected_nodes

    async def _submit_registration(
        self,
        host: str,
        port: int
    ):
        shard_id, response = await self.submit_registration(
            host,
            port
        )

        if isinstance(response, HealthCheck):
            source_host = response.source_host
            source_port = response.source_port

            not_self = self._check_is_not_self(
                source_host,
                source_port
            )

            self._instance_ids[(source_host, source_port)] = Snowflake.parse(shard_id).instance

            if not_self:
                self._node_statuses[(source_host, source_port)] = 'healthy'

            self._registered_counts[(source_host, source_port)] = max(
                response.registered_count,
                self._registered_counts[(source_host, source_port)]
            )

    @server()
    async def receive_vote_request(
        self,
        shard_id: int,
        raft_message: RaftMessage
    ) -> Call[RaftMessage]:

        source_host = raft_message.source_host
        source_port = raft_message.source_port

        term_number = raft_message.term_number

        elected_host: Union[str, None] = None
        elected_port: Union[int, None] = None

        if term_number > self._term_number:
            # The requesting node is ahead. They're elected the leader by default.
            elected_host = source_host
            elected_port = source_port

        elif term_number == self._term_number and self._raft_node_status != NodeState.LEADER:
            # The term numbers match, we can choose a candidate.

            elected_host, elected_port = self._get_max_instance_id()

        else:

            leader_host, leader_port = self._term_leaders[-1]

            return RaftMessage(
                host=source_host,
                port=source_port,
                source_host=self.host,
                source_port=self.port,
                elected_leader=(
                    leader_host,
                    leader_port
                ),
                status=self.status,
                error='Election request term cannot be less than current term.',
                vote_result=VoteResult.REJECTED,
                raft_node_status=self._raft_node_status,
                term_number=self._term_number
            )
            
        vote_result = VoteResult.REJECTED

        if elected_host == source_host and elected_port == source_port:
            vote_result = VoteResult.ACCEPTED

        return RaftMessage(
            host=source_host,
            port=source_port,
            source_host=self.host,
            source_port=self.port,
            elected_leader=(
                elected_host,
                elected_port
            ),
            status=self.status,
            vote_result=vote_result,
            raft_node_status=self._raft_node_status,
            term_number=term_number
        )
    
    @server()
    async def receive_log_update(
        self,
        shard_id: int,
        message: RaftMessage
    ) -> Call[RaftMessage]:
        
        entries_count = len(message.entries)

        if entries_count < 1:
            return RaftMessage(
                host=message.host,
                port=message.port,
                source_host=self.host,
                source_port=self.port,
                status=self.status,
                term_number=self._term_number,
                election_status=self._election_status,
                raft_node_status=self._raft_node_status
            )
        
        # We can use the Snowflake ID to sort since all records come from the 
        # leader.
        entries: List[Entry] = list(
            sorted(
                message.entries,
                key=lambda entry: Snowflake.parse(
                    entry.entry_id
                ).timestamp
            )
        )

        last_entry = entries[-1]

        leader_host = last_entry.leader_host
        leader_port = last_entry.leader_port
        
        try:


            if message.term_number > self._term_number:

                self._tasks_queue.append(
                    asyncio.create_task(
                        self._cancel_election(message)
                    )
                )
                
                amount_behind = max(
                    message.term_number - self._term_number - 1,
                    0
                )

                last_entry = entries[-1]

                leader_host = last_entry.leader_host
                leader_port = last_entry.leader_port

                self._term_number = message.term_number

                for _ in range(amount_behind):
                    self._term_leaders.append((
                        None,
                        None
                    ))

                await self._logger.distributed.aio.info(f'Term number for source - {self.host}:{self.port} - was updated to - {self._term_number} - and leader was updated to - {leader_host}:{leader_port}')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Term number for source - {self.host}:{self.port} - was updated to - {self._term_number} - and leader was updated to - {leader_host}:{leader_port}')


                self._term_leaders.append((
                    leader_host,
                    leader_port
                ))

                self._election_status = ElectionState.READY
                self._raft_node_status = NodeState.FOLLOWER

                return RaftMessage(
                    host=message.source_host,
                    port=message.source_port,
                    source_host=self.host,
                    source_port=self.port,
                    elected_leader=(
                        leader_host,
                        leader_port
                    ),
                    status=self.status,
                    error='Election request term cannot be less than current term.',
                    vote_result=VoteResult.REJECTED,
                    raft_node_status=self._raft_node_status,
                    term_number=self._term_number
                )


            source_host = message.source_host
            source_port = message.source_port

            if message.failed_node and self._suspect_tasks.get(
                message.failed_node
            ):
                
                node_host, node_port = message.failed_node
                
                self._tasks_queue.append(
                    asyncio.create_task(
                        self._cancel_suspicion_probe(
                            node_host,
                            node_port
                        )
                    )
                )

                await self._logger.distributed.aio.debug(f'Node - {node_host}:{node_port} - submitted healthy status to source - {self.host}:{self.port} - and is no longer suspect')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Node - {node_host}:{node_port} - submitted healthy status to source - {self.host}:{self.port} - and is no longer suspect')


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

                await self._logger.distributed.aio.debug(f'Node - {source_host}:{source_port} - submitted healthy status to source - {self.host}:{self.port} - and is no longer suspect')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Node - {source_host}:{source_port} - submitted healthy status to source - {self.host}:{self.port} - and is no longer suspect')

            error = self._logs.update(entries)

            self._local_health_multipliers[(source_host, source_port)] = self._reduce_health_multiplier(
                source_host,
                source_port
            )

            if isinstance(error, Exception):

                return RaftMessage(
                    host=message.source_host,
                    port=message.source_port,
                    source_host=self.host,
                    source_port=self.port,
                    status=self.status,
                    raft_node_status=self._raft_node_status,
                    error=str(error),
                    elected_leader=(
                        leader_host,
                        leader_port
                    ),
                    term_number=self._term_number
                )

            return RaftMessage(
                host=message.source_host,
                port=message.source_port,
                source_host=self.host,
                source_port=self.port,
                status=self.status,
                elected_leader=(
                    leader_host,
                    leader_port
                ),
                term_number=self._term_number,
                raft_node_status=self._raft_node_status,
                received_timestamp=self._logs.last_timestamp
            )
        
        except Exception as rpc_error:
            return RaftMessage(
                host=message.source_host,
                port=message.source_port,
                source_host=self.host,
                source_port=self.port,
                status=self.status,
                raft_node_status=self._raft_node_status,
                error=str(rpc_error),
                elected_leader=(
                    leader_host,
                    leader_port
                ),
                term_number=self._term_number
            )

    @server()
    async def receive_forwarded_entries(
        self,
        shard_id: int,
        message: RaftMessage
    ) -> Call[RaftMessage]:
        
        if self._raft_node_status == NodeState.LEADER and message.entries:
            
            entries = message.entries

            entries.append(
                Entry.from_data(
                    entry_id=self._entry_id_generator.generate(),
                    leader_host=self.host,
                    leader_port=self.port,
                    term=self._term_number,
                    data={
                        'key': 'logs_update',
                        'value': f'Node - {self.host}:{self.port} - submitted log update',
                    }
                )
            )

            self._tasks_queue.append(
                asyncio.create_task(
                    self._submit_logs_to_members(entries)
                )
            )

        return RaftMessage(
            host=message.host,
            port=message.port,
            source_host=self.host,
            source_port=self.port,
            status=self.status,
            term_number=self._term_number,
            raft_node_status=self._raft_node_status,
            received_timestamp=self._logs.last_timestamp
        )
        
    @server()
    async def receive_failure_notification(
        self,
        shard_id: int,
        message: RaftMessage
    ) -> Call[RaftMessage]:
        
        try:
            failed_node = message.failed_node
            host, port = failed_node

            not_self = self._check_is_not_self(
                host,
                port
            )


            if not_self and self._election_status == ElectionState.READY and failed_node not in self.failed_nodes:


                self.failed_nodes.append((
                    host,
                    port,
                    time.monotonic()
                ))

                self._node_statuses[failed_node] = 'failed'

                self._election_status = ElectionState.ACTIVE
                
                self._tasks_queue.append(
                    asyncio.create_task(
                        self.run_election(
                            failed_node=failed_node
                        )
                    )
                )

            return RaftMessage(
                host=message.host,
                port=message.port,
                source_host=self.host,
                source_port=self.port,
                status=self.status,
                term_number=self._term_number,
                raft_node_status=self._raft_node_status,
                received_timestamp=self._logs.last_timestamp
            )
        
        except Exception:
            pass

    @client('receive_vote_request')
    async def request_vote(
        self,
        host: str,
        port: int
    ) -> Call[RaftMessage]:
        return RaftMessage(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            status=self.status,
            term_number=self._term_number,
            raft_node_status=self._raft_node_status
        )
    
    @client('receive_log_update')
    async def submit_log_update(
        self,
        host: str,
        port: int,
        entries: List[Entry],
        failed_node: Optional[Tuple[str, int]]=None
    ) -> Call[RaftMessage]:
        return RaftMessage(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            status=self.status,
            term_number=self._term_number,
            raft_node_status=self._raft_node_status,
            failed_node=failed_node,
            entries=entries
        )
    
    @client('receive_forwarded_entries')
    async def forward_entries_to_leader(
        self,
        host: str,
        port: int,
        entries: List[Entry]
    ) -> Call[RaftMessage]:
        return RaftMessage(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            status=self.status,
            term_number=self._term_number,
            raft_node_status=self._raft_node_status,
            entries=entries
        )
    
    @client('receive_failure_notification')
    async def submit_failure_notification(
        self,
        host: str,
        port: int,
        failed_node: Tuple[str, int]
    ) -> Call[RaftMessage]:
        return RaftMessage(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            status=self.status,
            term_number=self._term_number,
            raft_node_status=self._raft_node_status,
            failed_node=failed_node
        )
            
    async def _start_suspect_monitor(self):
        suspect_host, suspect_port = await super()._start_suspect_monitor()

        node_status = self._node_statuses.get((suspect_host, suspect_port))

        failed_node = (suspect_host, suspect_port)

        if self._election_status == ElectionState.READY and node_status == 'failed' and failed_node not in self.failed_nodes:

            self.failed_nodes.append((
                suspect_host,
                suspect_port,
                time.monotonic()
            ))

            self._election_status = ElectionState.ACTIVE

            await self.notify_of_failed_node(failed_node=failed_node)
            await self.run_election(failed_node=failed_node)

    async def push_entries(
        self,
        entries: List[Dict[str, Any]]
    ) -> List[RaftMessage]:

        entries.append({
            'key': 'logs_update',
            'value': f'Node - {self.host}:{self.port} - submitted log update',
        })

        entries = self._convert_data_to_entries(entries)
        entries_count = len(entries)

        if self._raft_node_status == NodeState.LEADER:
            results = await self._submit_logs_to_members(entries)

            results_count = len(results)

            await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - pushed - {entries_count} - entries to - {results_count} - members')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - pushed - {entries_count} - entries to - {results_count} - members')

            return results

        else:

            try:

                current_leader_host, current_leader_port = self._term_leaders[-1]

                result = await asyncio.wait_for(
                    self.forward_entries_to_leader(
                        current_leader_host,
                        current_leader_port,
                        entries
                    ),
                    timeout=self._calculate_current_timeout(
                        current_leader_host,
                        current_leader_port
                    )
                )

                await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - forwarded - {entries_count} - entries to leader at - {current_leader_host}:{current_leader_port}')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - forwarded - {entries_count} - entries to leader at - {current_leader_host}:{current_leader_port}')

                return [
                    result
                ]
                
            except Exception as forward_error:
                
                await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - encountered error - {str(forward_error)} - out forwarding - {entries_count} - entries to leader at - {current_leader_host}:{current_leader_port}')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - encountered error - {str(forward_error)} - out forwarding - {entries_count} - entries to leader at - {current_leader_host}:{current_leader_port}')
        
                return [
                    RaftMessage(
                        host=current_leader_host,
                        port=current_leader_port,
                        source_host=self.host,
                        source_port=self.port,
                        elected_leader=(
                            current_leader_host,
                            current_leader_port
                        ),
                        error=str(forward_error),
                        raft_node_status=self._raft_node_status,
                        status=self.status,
                        term_number=self._term_number
                    )
                ]
            
    def submit_entries(
        self,
        entries: List[Dict[str, Any]]
    ):
        self._tasks_queue.append(
            asyncio.create_task(
                self.push_entries(entries)
            )
        )

    def _convert_data_to_entries(
        self,
        entries: List[Dict[str, Any]]
    ) -> List[Entry]:
        
        current_leader_host, current_leader_port = self._term_leaders[-1]

        entries = [
            Entry.from_data(
                self._entry_id_generator.generate(),
                current_leader_host,
                current_leader_port,
                self._term_number,
                entry
            ) for entry in entries
        ]

        return entries
    
    def _get_max_instance_id(self):

        nodes = [
            address for address, status in self._node_statuses.items() if status == 'healthy'
        ]

        nodes.append((
            self.host,
            self.port
        ))

        instance_address_id_pairs = list(
            sorted(
                nodes,
                key=lambda instance: self._instance_ids.get(
                    instance,
                    self._instance_id
                )
            )
        )

        if len(instance_address_id_pairs) > 0:

            max_instance = instance_address_id_pairs[-1]
            elected_host, elected_port = max_instance

        else:
            
            elected_host = self.host
            elected_port = self.port

        return elected_host, elected_port

    async def _cancel_election(
        self,
        message: RaftMessage
    ):
        self._election_status = ElectionState.READY
        self._term_number = message.term_number

        if self._election_task:
            await cancel(self._election_task)
            self._election_task = None

            await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - election for term - {self._term_number} - was cancelled due to leader reporting for term')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - election for term - {self._term_number} - was cancelled due to leader reporting for term')

    async def _update_logs(
        self,
        host: str,
        port: int,
        entries: List[Entry],
        failed_node: Optional[Tuple[str, int]]=None,
    ) -> Union[
        Tuple[int, RaftMessage],
        None
    ]:
        shard_id: Union[int, None] = None
        update_response: Union[RaftMessage, None] = None

        await self._logger.distributed.aio.debug(f'Running UDP logs update for node - {host}:{port} - for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Running UDP logs update for node - {host}:{port} - for source - {self.host}:{self.port}')
        
        for idx in range(self._poll_retries):

            try:

                response = await asyncio.wait_for(
                    self.submit_log_update(
                        host,
                        port,
                        entries,
                        failed_node=failed_node
                    ),
                    timeout=self._calculate_current_timeout(
                        host,
                        port
                    )
                )

                shard_id, update_response = response
                source_host, source_port = update_response.source_host, update_response.source_port

                not_self = self._check_is_not_self(
                    source_host,
                    source_port
                )

                if not_self:
                    self._node_statuses[(source_host, source_port)] = update_response.status

                self._local_health_multipliers[(host, port)] = self._reduce_health_multiplier(
                    host,
                    port
                )

                return shard_id, update_response
            
            except Exception:

                self._local_health_multipliers[(host, port)] = self._increase_health_multiplier(
                    host,
                    port
                )

        check_host = host
        check_port = port

        node_status = self._node_statuses.get((
            check_host, 
            check_port
        )) 

        not_self = self._check_is_not_self(
            check_host,
            check_port
        )

        if not_self and update_response is None and node_status == 'healthy':

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

        return self._poll_timeout * (self._local_health_multipliers[(host, port)] + 1) * modifier
    
    async def notify_of_failed_node(
        self,
        failed_node: Tuple[str, int]
    ):
        monitors = [
            address for address, status in self._node_statuses.items() if status == 'healthy' and address != failed_node
        ]

        responses: List[
            Union[
                Tuple[int, RaftMessage],
                Exception
            ]
        ] = await asyncio.gather(*[
            asyncio.wait_for(
                self.submit_failure_notification(
                    host,
                    port,
                    failed_node
                ),
                timeout=self._calculate_current_timeout(
                    host,
                    port
                )
            ) for host , port in monitors
        ], return_exceptions=True)

        for response in responses:
            if isinstance(response, Exception):
                raise response
    
    
    async def run_election(
        self,
        failed_node: Optional[Tuple[str, int]]=None
    ):

        # Trigger new election
        next_term = self._term_number + 1
        self._raft_node_status = NodeState.CANDIDATE

        await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - Running election for term - {next_term}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - Running election for term - {next_term}')

        elected_host, elected_port = self._get_max_instance_id()
        self._term_leaders.append((elected_host, elected_port))

        if elected_host == self.host and elected_port == self.port:

            await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - was elected as leader for term - {next_term}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - was elected as leader for term - {next_term}')


            self._raft_node_status = NodeState.LEADER
            self._term_number += 1

            members: List[Tuple[str, int]] = [
                address for address, status in self._node_statuses.items() if status == 'healthy'
            ]

            members = list(set(members))

            self._logs.update([
                Entry.from_data(
                    entry_id=self._entry_id_generator.generate(),
                    leader_host=self.host,
                    leader_port=self.port,
                    term=self._term_number,
                    data={
                        'key': 'election_update',
                        'value': f'Election complete! Elected - {self.host}:{self.port}'
                    }
                )
            ])

            members: List[Tuple[str, int]] = [
                address for address, status in self._node_statuses.items() if status == 'healthy'
            ]

            latest_logs = self._logs.latest()

            await asyncio.gather(*[
                asyncio.wait_for(
                    self._update_logs(
                        host,
                        port,
                        latest_logs,
                        failed_node=failed_node
                    ),
                    timeout=self._calculate_current_timeout(
                        host,
                        port
                    )
                ) for host, port in members
            ], return_exceptions=True)

        else:

            self._raft_node_status = NodeState.FOLLOWER

            await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - failed to receive majority votes and is reverting to a follower for term - {self._term_number}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - failed to receive majority votes and is reverting to a follower for term - {self._term_number}')

        if self._term_number > next_term:
            self._term_number = next_term
        
        self._election_status = ElectionState.READY
        
        return

    async def _run_raft_monitor(self):

        while self._running:

            if self._raft_node_status == NodeState.LEADER:

                self._tasks_queue.append(
                    asyncio.create_task(
                        self._submit_logs_to_members([
                            Entry.from_data(
                                entry_id=self._entry_id_generator.generate(),
                                leader_host=self.host,
                                leader_port=self.port,
                                term=self._term_number,
                                data={
                                    'key': 'logs_update',
                                    'value': f'Node - {self.host}:{self.port} - submitted log update',
                                }
                            )
                        ])
                    )
                )

            await asyncio.sleep(
                self._logs_update_poll_interval * self._initial_expected_nodes
            )

    async def _submit_logs_to_members(
        self,
        entries: List[Entry]
    ) -> List[RaftMessage]:
         
        members: List[Tuple[str, int]] = [
            address for address, status in self._node_statuses.items() if status == 'healthy'
        ]

        self._logs.update(entries)

        latest_logs = self._logs.latest()

        results: List[Tuple[
            int,
            RaftMessage
        ]] = await asyncio.gather(*[
            asyncio.create_task(
                self._update_logs(
                    host,
                    port,
                    latest_logs
                )
            ) for host, port in members
        ])

        self._logs.commit()

        return results
    
    async def _cleanup_pending_raft_tasks(self):

        await self._logger.distributed.aio.debug(f'Running cleanup for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Running cleanup for source - {self.host}:{self.port}')

        while self._running:

            pending_count = 0

            for pending_task in list(self._tasks_queue):
                if pending_task.done() or pending_task.cancelled():
                    try:
                        await pending_task

                    except Exception:
                        pass

                    self._tasks_queue.remove(pending_task)
                    pending_count += 1

            await self._logger.distributed.aio.debug(f'Cleaned up - {pending_count} - for source - {self.host}:{self.port}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Cleaned up - {pending_count} - for source - {self.host}:{self.port}')

            await asyncio.sleep(self._logs_update_poll_interval)
            self._logs.prune()

    async def leave(self):
        await self._logger.distributed.aio.debug(f'Shutdown requested for RAFT source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Shutdown requested for RAFT source - {self.host}:{self.port}')

        await cancel(self._raft_monitor_task)
        await cancel(self._raft_cleanup_task)

        if self._election_task:
            await cancel(self._election_task)
            self._election_task = None

        await self._submit_leave_requests()
        await self._shutdown()

        await self._logger.distributed.aio.debug(f'Shutdown complete for RAFT source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Shutdown complete for RAFT source - {self.host}:{self.port}')
