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
    SnowflakeGenerator
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

from .constants import FLEXIBLE_PAXOS_QUORUM
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

        self._term_number = 0
        self._term_votes = defaultdict(
            lambda: defaultdict(
                lambda: 0
            )
        )

        self._initial_expected_nodes = replication_env.MERCURY_SYNC_RAFT_EXPECTED_NODES

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

        max_wait = self.registration_timeout * self._initial_expected_nodes

        await self._register_initial_node()

        self.status = 'healthy'
        self._running = True

        self._healthchecks[(self.bootstrap_host, self.bootstrap_port)] = asyncio.create_task(
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

        await asyncio.wait_for(
            self._wait_for_nodes(),
            timeout=max_wait
        )
        
        self._raft_cleanup_task = asyncio.create_task(
            self._cleanup_pending_raft_tasks()
        )

        self._raft_monitor_task = asyncio.create_task(
            self._run_raft_monitor()
        )

        boot_wait = random.uniform(0.1, self.boot_wait)
        await asyncio.sleep(boot_wait)

        if self._term_number == 0:

            while self._term_number < 1:

                self._election_status = ElectionState.ACTIVE

                if self._election_task is None or self._election_task.done() or self._election_task.cancelled():
                    self._election_task = asyncio.create_task(
                        self.run_election()
                    )

                await asyncio.sleep(self._election_poll_interval)

            if self._election_task and not self._election_task.done() and not self._election_task.cancelled():
                await cancel(self._election_task)
                self._election_task = None

    async def _wait_for_nodes(self):

        currently_registered = len(self._node_statuses) + 1
        poll_interval_offset = 0

        while currently_registered < self._initial_expected_nodes:

            await self._logger.distributed.aio.info(f'Waiting start - {self.host}:{self.port} - with - {currently_registered}/{self._initial_expected_nodes} - registered')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Waiting start - {self.host}:{self.port} - with - {currently_registered}/{self._initial_expected_nodes} - registered')

            await asyncio.sleep(self._poll_interval + poll_interval_offset)

            currently_registered = len(self._node_statuses) + 1

            poll_interval_offset = self._initial_expected_nodes/max(currently_registered, 1)

    async def _register_initial_node(self):
        await self._logger.distributed.aio.info(f'Connecting to initial node - {self.bootstrap_host}:{self.bootstrap_port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Connecting to initial node - {self.bootstrap_host}:{self.bootstrap_port}')

        try:

            await asyncio.wait_for(
                asyncio.create_task(
                    self.start_client(
                        {
                            (self.bootstrap_host, self.bootstrap_port): [
                                HealthCheck,
                                RaftMessage
                            ]
                        },
                        cert_path=self.cert_path,
                        key_path=self.key_path
                    )
                ),
                timeout=self.registration_timeout
            )

            self._node_statuses[(self.bootstrap_host, self.bootstrap_port)] = 'healthy'

        except Exception:
            pass

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

            members: List[Tuple[str, int]] = [
                address for address, status in self._node_statuses.items() if status == 'healthy'
            ]

            elected_host, elected_port = random.choice(
                list(set(members))
            )

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
                key=lambda entry: entry.entry_id
            )
        )

        current_leader_host, current_leader_port = self._get_current_term_leader()

        leader_host = current_leader_host
        leader_port = current_leader_port

        last_entry = entries[-1]

        leader_host = last_entry.leader_host
        leader_port = last_entry.leader_port


        if message.term_number > self._term_number:

            self._tasks_queue.append(
                asyncio.create_task(
                    self._cancel_election(message)
                )
            )

            suspect_tasks = dict(self._suspect_tasks)
            suspect_task = suspect_tasks.pop(message.failed_node, None)

            if suspect_task:
                await cancel(suspect_task)
            
            amount_behind = max(
                message.term_number - self._term_number - 1,
                0
            )

            last_entry = entries[-1]

            leader_host = last_entry.leader_host
            leader_port = last_entry.leader_port

            for _ in range(amount_behind):
                self._term_leaders.append((
                    None,
                    None
                ))

            self._term_leaders.append((
                leader_host,
                leader_port
            ))
            
            await self._logger.distributed.aio.info(f'Term number for source - {self.host}:{self.port} - was updated to - {self._term_number} - and leader was updated to - {leader_host}:{leader_port}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Term number for source - {self.host}:{self.port} - was updated to - {self._term_number} - and leader was updated to - {leader_host}:{leader_port}')

            self._election_status = ElectionState.READY
            self._raft_node_status = NodeState.FOLLOWER

        elif leader_host != current_leader_host and leader_port != current_leader_port and self._term_number == message.term_number:
            
            self._tasks_queue.append(
                asyncio.create_task(
                    self._cancel_election(message)
                )
            )

            self._election_status = ElectionState.READY
            self._raft_node_status = NodeState.FOLLOWER

            self._term_leaders[-1] = (
                leader_host,
                leader_port
            )

            await self._logger.distributed.aio.info(f'Leader for source - {self.host}:{self.port} - was updated to - {leader_host}:{leader_port} - for term - {self._term_number}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Leader for source - {self.host}:{self.port} - was updated to - {leader_host}:{leader_port} - for term - {self._term_number}')

        else:

            leader_host, leader_port = self._term_leaders[-1]

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
        elected_leader = self._get_current_term_leader()

        self._local_health_multipliers[(source_host, source_port)] = max(
            0, 
            self._local_health_multipliers[(source_host, source_port)] - 1
        )

        if isinstance(error, Exception):

            return RaftMessage(
                host=message.host,
                port=message.port,
                source_host=self.host,
                source_port=self.port,
                status=self.status,
                raft_node_status=self._raft_node_status,
                error=str(error),
                elected_leader=elected_leader,
                term_number=self._term_number
            )

        return RaftMessage(
            host=message.host,
            port=message.port,
            source_host=self.host,
            source_port=self.port,
            status=self.status,
            elected_leader=elected_leader,
            term_number=self._term_number,
            raft_node_status=self._raft_node_status,
            received_timestamp=self._logs.last_timestamp
        )

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
    
    async def _start_suspect_monitor(self):
        suspect_host, suspect_port = await super()._start_suspect_monitor()


        can_start_election = self._election_task is None or self._election_task.done() or self._election_task.cancelled()
        
        if self._election_status == ElectionState.READY and can_start_election:
            self._election_status = ElectionState.ACTIVE
            self._election_task = asyncio.create_task(
                self.run_election(
                    failed_node=(
                        suspect_host,
                        suspect_port
                    )
                )
            )

    async def submit_entries(
        self,
        entries: List[Dict[str, Any]]
    ):
        
        if self._raft_node_status == NodeState.LEADER:

            self._logs.update(entries)

            members = [
                address for address, status in self._node_statuses.items() if status == 'healthy'
            ]

            latest_logs = self._logs.latest()

            for host, port in members:
                self._tasks_queue.append(
                    asyncio.create_task(
                        self._update_logs(
                            host,
                            port,
                            latest_logs
                        )
                    )
                )

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

    async def _cancel_suspicion_probe(
        self,
        message: RaftMessage
    ):
        suspect_tasks = dict(self._suspect_tasks)
        suspect_task = suspect_tasks.pop(message.failed_node, None)

        if suspect_task:
            await cancel(suspect_task)
            del self._suspect_tasks[message.failed_node]

            self._suspect_tasks = suspect_tasks
        
    async def _update_logs(
        self,
        host: str,
        port: int,
        entries: List[Entry],
        failed_node: Optional[Tuple[str, int]]=None
    ):
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

                self._node_statuses[(source_host, source_port)] = update_response.status

                self._local_health_multipliers[(host, port)] = max(
                    0, 
                    self._local_health_multipliers[(host, port)] - 1
                )

                elected_leader_host, elected_leader_port = update_response.elected_leader
                current_leader_host, current_leader_port = self._get_current_term_leader()

                elected_leader_matches = current_leader_host == elected_leader_host and current_leader_port == elected_leader_port

                if update_response.error and elected_leader_matches is False:

                    await self._cancel_election(update_response)

                    if update_response.failed_node and self._suspect_tasks.get(
                        update_response.failed_node
                    ):

                        node_host, node_port = update_response.failed_node

                        self._tasks_queue.append(
                            asyncio.create_task(
                                self._cancel_suspicion_probe(
                                    node_host,
                                    node_port
                                )
                            )
                        )

                    self._term_leaders.append(update_response.elected_leader)
                    self._term_number = update_response.term_number

                return shard_id, update_response
            
            except asyncio.TimeoutError:

                await self._refresh_after_timeout(
                    host,
                    port
                )

                self._local_health_multipliers[(host, port)] = min(
                    self._local_health_multipliers[(host, port)], 
                    self._max_poll_multiplier
                ) + 1

        check_host = host
        check_port = port

        if update_response is None and self._node_statuses.get((check_host, check_port)) == 'healthy':

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

            await self._logger.distributed.aio.debug(f'Node - {check_host}:{check_port} - responded on try - {idx}/{self._poll_interval} - for source - {self.host}:{self.port}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Node - {check_host}:{check_port} - responded on try - {idx}/{self._poll_interval} - for source - {self.host}:{self.port}')

    def _calculate_current_timeout(
        self,
        host: str,
        port: int
    ):
        
        monitors = [
            address for address, status in self._node_statuses.items() if status == 'healthy'
        ]

        return self._poll_timeout * (self._local_health_multipliers[(host, port)] + 1) * len(monitors)

    def _get_current_term_leader(self):

        current_leader_index = max(
            self._term_number,
            0
        )

        if current_leader_index < len(self._term_leaders):
            current_leader_host, current_leader_port = self._term_leaders[current_leader_index]

        elif len(self._term_leaders) > 0:
            current_leader_host, current_leader_port = self._term_leaders[-1]

        else:
            current_leader_host = self.host
            current_leader_port = self.port

        return (
            current_leader_host,
            current_leader_port
        )
    
    async def run_election(
        self,
        failed_node: Optional[Tuple[str, int]]=None
    ):

        # Trigger new election
        next_term = self._term_number + 1
        self._raft_node_status = NodeState.CANDIDATE

        while self._term_number < next_term and self._raft_node_status == NodeState.CANDIDATE:

            await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - Running election for term - {next_term}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - Running election for term - {next_term}')

  
            self._term_votes[self._term_number][(self.host, self.port)] = 1

            members: List[Tuple[str, int]] = [
                address for address, status in self._node_statuses.items() if status == 'healthy'
            ]

            members = list(set(members))

            quorum_count = max(
                int(
                    len(members) * FLEXIBLE_PAXOS_QUORUM
                ) + 1,
                1
            )

            try:

                election_timeout = random.uniform(
                    self._min_election_timeout,
                    self._max_election_timeout
                )

                for result in asyncio.as_completed([
                    self.request_vote(
                        member_host,
                        member_port
                    ) for member_host, member_port in members[:quorum_count]
                ], timeout=election_timeout):

                    vote_result: Tuple[int, RaftMessage] = await result

                    (
                        _, 
                        vote
                    ) = vote_result

                    leader_host, leader_port = vote.elected_leader

                    if vote.vote_result == VoteResult.ACCEPTED:
                        self._term_votes[self._term_number][(self.host, self.port)] += 1

                    elif vote.term_number > self._term_number and leader_host and leader_port:

                        self._term_number = vote.term_number
                        self._term_leaders.append((
                            leader_host,
                            leader_port
                        ))

                        next_term = self._term_number + 1
                        
                        await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - was behind a term and is now a follower for term - {self._term_number}')
                        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - as behind a term and is now a follower for term - {self._term_number}')

                        return

            except asyncio.TimeoutError:
                
                await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - timed out for candidate term - {next_term} - reverting to term - {self._term_number}')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - timed out for candidate term - {next_term} - reverting to term - {self._term_number}')

            except Exception:
                await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - encountered error for candidate term - {next_term} - reverting to term - {self._term_number}')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - encountered error for candidate term - {next_term} - reverting to term - {self._term_number}')

        
            accepted_count = self._term_votes[self._term_number][(self.host, self.port)]

            await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - received - {accepted_count} - votes')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - received - {accepted_count} - votes')


            if accepted_count >= quorum_count and self._election_status == ElectionState.ACTIVE:

                # We're the leader!
                await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - was elected as leader for term - {next_term}')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - was elected as leader for term - {next_term}')


                self._raft_node_status = NodeState.LEADER
                self._term_leaders.append((self.host, self.port))
                self._term_number += 1

                members: List[Tuple[str, int]] = [
                    address for address, status in self._node_statuses.items() if status == 'healthy'
                ]

                members = list(set(members))

                current_leader_host, current_leader_port = self._get_current_term_leader()

                self._logs.update([
                    Entry.from_data(
                        entry_id=self._entry_id_generator.generate(),
                        leader_host=current_leader_host,
                        leader_port=current_leader_port,
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

                for host, port in members:
                    self._tasks_queue.append(
                        asyncio.create_task(
                            self._update_logs(
                                host,
                                port,
                                latest_logs,
                                failed_node=failed_node
                            )
                        )
                    )

            else:

                self._raft_node_status = NodeState.FOLLOWER

                await self._logger.distributed.aio.info(f'Source - {self.host}:{self.port} - failed to receive majority votes and is now a follower for term - {next_term}')
                await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - failed to receive majority votes and is now a follower for term - {next_term}')

            if self._term_number > next_term:
                self._term_number = next_term
                self._election_status = ElectionState.READY
                return

            if self._term_number == next_term:
                self._election_status = ElectionState.READY
                return
            
            else:
                self._raft_node_status = NodeState.CANDIDATE

    async def _run_raft_monitor(self):

        while self._running:

            members: List[Tuple[str, int]] = [
                address for address, status in self._node_statuses.items() if status == 'healthy'
            ]

            if self._raft_node_status == NodeState.LEADER:

                current_leader_host, current_leader_port = self._get_current_term_leader()

                self._logs.update([
                    Entry.from_data(
                        entry_id=self._entry_id_generator.generate(),
                        leader_host=current_leader_host,
                        leader_port=current_leader_port,
                        term=self._term_number,
                        data={
                            'key': 'logs_update',
                            'value': f'Node - {self.host}:{self.port} - submitted log update',
                        }
                    )
                ])

                latest_logs = self._logs.latest()

                for host, port in members:
            
                    self._tasks_queue.append(
                        asyncio.create_task(
                            self._update_logs(
                                host,
                                port,
                                latest_logs
                            )
                        )
                    )

            current_leader_host, current_leader_port = self._get_current_term_leader()

            await self._logger.distributed.aio.debug(f'Source - {self.host}:{self.port} - has node {current_leader_host}:{current_leader_port} - as leader for term - {self._term_number}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].info(f'Source - {self.host}:{self.port} - has node {current_leader_host}:{current_leader_port} - as leader for term - {self._term_number}')

            await asyncio.sleep(
                self._logs_update_poll_interval
            )

    async def _cleanup_pending_raft_tasks(self):

        await self._logger.distributed.aio.debug(f'Running cleanup for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Running cleanup for source - {self.host}:{self.port}')

        while self._running:

            pending_count = 0

            for pending_task in list(self._tasks_queue):
                if pending_task.done() or pending_task.cancelled():
                    try:
                        await pending_task

                    except (
                        ConnectionRefusedError,
                        ConnectionAbortedError,
                        ConnectionResetError
                    ):
                        pass

                    self._tasks_queue.remove(pending_task)
                    pending_count += 1

            await self._logger.distributed.aio.debug(f'Cleaned up - {pending_count} - for source - {self.host}:{self.port}')
            await self._logger.filesystem.aio[f'hedra.distributed.{self._instance_id}'].debug(f'Cleaned up - {pending_count} - for source - {self.host}:{self.port}')

            await asyncio.sleep(self._logs_update_poll_interval)

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
