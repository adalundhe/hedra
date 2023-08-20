import asyncio
import traceback
import random
from collections import (
    defaultdict,
    deque
)
from hedra.distributed.env import (
    Env, 
    RaftEnv,
    load_env
)
from hedra.distributed.env.time_parser import TimeParser
from hedra.distributed.hooks.client_hook import client
from hedra.distributed.hooks.server_hook import server
from hedra.distributed.models.raft.raft_message import (
    RaftMessage
)
from hedra.distributed.types import Call
from hedra.distributed.models.healthcheck import HealthCheck
from hedra.distributed.models.raft.election_state import ElectionState
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


class RaftController(Monitor):

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

        raft_env = load_env(RaftEnv) 

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

        self._max_election_timeout = TimeParser(
            raft_env.MERCURY_SYNC_RAFT_ELECTION_MAX_TIMEOUT
        ).time

        self._min_election_timeout = max(
            self._max_election_timeout * 0.5,
            1
        )

        self._election_poll_interval = TimeParser(
            raft_env.MERCURY_SYNC_RAFT_ELECTION_POLL_INTERVAL
        ).time

        self._logs_update_poll_interval = TimeParser(
            raft_env.MERCURY_SYNC_RAFT_LOGS_UPDATE_POLL_INTERVAL
        ).time
        
        self._election_status = ElectionState.READY
        self._raft_node_status = NodeState.FOLLOWER
        self._active_election_waiter: Union[asyncio.Future, None] = None
        self._latest_election: Dict[int, int] = {}
        self._term_leaders: List[Tuple[str, int]] = []
        
        self._running = False

        self._logs = LogQueue()
        self._previous_entry_index = 0
        self._last_timestamp = 0
        self._last_commit_timestamp = 0
        self._term_number = 0

        self._raft_monitor_task: Union[asyncio.Task, None] = None
        self._tasks_queue: Deque[asyncio.Task] = deque()
        self._entry_id_generator = SnowflakeGenerator(self._instance_id)

        logging_manager.logfiles_directory = logs_directory
        logging_manager.update_log_level(
            'info'
        )

        self._logger = HedraLogger()
        self._logger.initialize()


        self._election_poll_interval = TimeParser(
            raft_env.MERCURY_SYNC_RAFT_ELECTION_POLL_INTERVAL
        ).time

        self._cleanup_interval = TimeParser(
            env.MERCURY_SYNC_CLEANUP_INTERVAL
        ).time

        self._pending_election_waiter: Union[asyncio.Future, None]  = None

        self._election_timeout = random.uniform(
            self._min_election_timeout,
            self._max_election_timeout
        )

        self._raft_cleanup_task: Union[asyncio.Future, None] = None
        
    async def start(
        self,
        boot_wait: Optional[float]=None,
        skip_boot_wait: bool=False
    ):
        
        if boot_wait is None:
            boot_wait = self.boot_wait

        await self._logger.filesystem.aio.create_logfile('hedra.distributed.log')
        self._logger.filesystem.create_filelogger('hedra.distributed.log')

        await self.start_server()

        if skip_boot_wait is False:
            await asyncio.sleep(boot_wait)

        loop = asyncio.get_event_loop()
        self._last_commit_timestamp = loop.time()

    async def register(
        self,
        host: str,
        port: int
    ):
    
        await self._logger.distributed.aio.info(f'Initializing node - {self.host}:{self.port}')
        await self._logger.filesystem.aio['hedra.distributed'].info(f'Initializing node - {self.host}:{self.port}')

        
        self.bootstrap_host = host
        self.bootstrap_port = port


        await self._logger.distributed.aio.info(f'Connecting to node node - {self.bootstrap_host}:{self.bootstrap_port}')
        await self._logger.filesystem.aio['hedra.distributed'].info(f'Connecting to node node - {self.bootstrap_host}:{self.bootstrap_port}')

        
        await asyncio.wait_for(
            asyncio.create_task(
                self.start_client(
                    {
                        (host, port): [
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

        self._node_statuses[(host, port)] = 'healthy'

        self.status = 'healthy'
        self._running = True
        
        self._healthchecks[(host, port)] = asyncio.create_task(
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
        await self._logger.filesystem.aio['hedra.distributed'].info(f'Initialized node - {self.host}:{self.port}')

        self._tasks_queue.append(
            asyncio.create_task(
                self.run_election(
                    host,
                    port
                )
            )
        )

        self._raft_monitor_task = asyncio.create_task(
            self._run_raft_monitor()
        )
        
        self._raft_cleanup_task = asyncio.create_task(
            self._cleanup_pending_raft_tasks()
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

        if self._election_status in [ElectionState.ACTIVE, ElectionState.PENDING]:
            # There is already an election in play
            election_result = RaftMessage(
                host=source_host,
                port=source_port,
                source_host=self.host,
                source_port=self.port,
                status=self.status,
                election_status=ElectionState.REJECTED,
                raft_node_status=self._raft_node_status,
                term_number=term_number
            )
            

        elif term_number > self._term_number:

            self._election_status = ElectionState.ACTIVE
            # The requesting node is ahead. They're elected the leader by default.
            elected_host = source_host
            elected_port = source_port
            self._term_number = term_number

        elif term_number == self._term_number:
            # The term numbers match, we can choose a candidate.

            self._election_status = ElectionState.ACTIVE
            members: List[Tuple[str, int]] = [
                address for address, status in self._node_statuses.items() if status == 'healthy'
            ]

            elected_host, elected_port = random.choice(
                list(set(members))
            )

        else:

            election_result = RaftMessage(
                host=source_host,
                port=source_port,
                source_host=self.host,
                source_port=self.port,
                status=self.status,
                election_status=ElectionState.REJECTED,
                raft_node_status=self._raft_node_status,
                term_number=term_number
            )
            
        
        if elected_host == source_host and elected_port == source_port:
            
            election_result = RaftMessage(
                host=source_host,
                port=source_port,
                source_host=self.host,
                source_port=self.port,
                status=self.status,
                election_status=ElectionState.ACCEPTED,
                raft_node_status=self._raft_node_status,
                term_number=term_number
            )
        
        elif elected_host is not None and elected_port is not None:

            election_result = RaftMessage(
                host=source_host,
                port=source_port,
                source_host=self.host,
                source_port=self.port,
                status=self.status,
                election_status=ElectionState.REJECTED,
                raft_node_status=self._raft_node_status,
                term_number=term_number
            )

        return election_result
    
    @server()
    async def receive_log_update(
        self,
        shard_id: int,
        message: RaftMessage
    ) -> Call[RaftMessage]:
        
        entries_count = len(message.entries)

        if entries_count < 0:
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

        if len(entries) > 0 and message.term_number > self._term_number:
            self._term_number = message.term_number

            last_entry = entries[-1]

            leader_host = last_entry.leader_host
            leader_port = last_entry.leader_port


        if leader_host != current_leader_host and leader_port != current_leader_port:
            self._term_leaders.append((
                leader_host,
                leader_port
            ))

            self._raft_node_status = NodeState.FOLLOWER

        source_host = message.source_host
        source_port = message.source_port

        suspect_tasks = dict(self._suspect_tasks)
        suspect_task = suspect_tasks.pop((source_host, source_port), None)

        if suspect_task:

            await self._logger.distributed.aio.debug(f'Node - {source_host}:{source_port} - submitted healthy status to source - {self.host}:{self.port} - and is no longer suspect')
            await self._logger.filesystem.aio['hedra.distributed'].debug(f'Node - {source_host}:{source_port} - submitted healthy status to source - {self.host}:{self.port} - and is no longer suspect')

            self._tasks_queue.append(
                asyncio.create_task(
                    cancel(suspect_task)
                )
            )

            del self._suspect_tasks[(source_host, source_port)]

            self._suspect_tasks = suspect_tasks

        error = self._logs.update(entries)
        elected_leader = self._get_current_term_leader()

        if isinstance(error, Exception):

            return RaftMessage(
                host=message.host,
                port=message.port,
                source_host=self.host,
                source_port=self.port,
                status=self.status,
                election_status=self._election_status,
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
            election_status=self._election_status,
            raft_node_status=self._raft_node_status
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
            election_status=self._election_status,
            raft_node_status=self._raft_node_status
        )
    
    @client('receive_log_update')
    async def submit_log_update(
        self,
        host: str,
        port: int,
        entries: List[Dict[str, Any]]
    ) -> Call[RaftMessage]:
        
        leader_host, leader_port = self._get_current_term_leader()

        return RaftMessage(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            status=self.status,
            term_number=self._term_number,
            election_status=self._election_status,
            raft_node_status=self._raft_node_status,
            entries=[
                Entry(
                    entry_id=self._entry_id_generator.generate(),
                    term=self._term_number,
                    leader_host=leader_host,
                    leader_port=leader_port,
                    **entry
                ) for entry in entries
            ]
        )
    
    async def _update_logs(
        self,
        host: str,
        port: int,
        entries: List[Dict[str, Any]]
    ):
        shard_id: Union[int, None] = None
        update_response: Union[RaftMessage, None] = None

        await self._logger.distributed.aio.debug(f'Running UDP logs update for node - {host}:{port} - for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio['hedra.distributed'].debug(f'Running UDP logs update for node - {host}:{port} - for source - {self.host}:{self.port}')

        
        for idx in range(self._poll_retries):

            if self._node_statuses[(host, port)] == 'healthy':

                try:

                    response = await asyncio.wait_for(
                        self.submit_log_update(
                            host,
                            port,
                            entries
                        ),
                        timeout=self._poll_timeout * (self._local_health_multipliers[(host, port)] + 1)
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
                        self._term_leaders.append(update_response.elected_leader)
                        self._term_number = update_response.term_number

                    return shard_id, update_response
                
                except asyncio.TimeoutError:
                    self._local_health_multipliers[(host, port)] = min(
                        self._local_health_multipliers[(host, port)], 
                        self._max_poll_multiplier
                    ) + 1

        check_host = host
        check_port = port

        if update_response is None and self._node_statuses.get((check_host, check_port)) == 'healthy':

            await self._logger.distributed.aio.debug(f'Node - {check_host}:{check_port} - failed to respond over - {self._poll_retries} - retries and is now suspect for source - {self.host}:{self.port}')
            await self._logger.filesystem.aio['hedra.distributed'].info(f'Node - {check_host}:{check_port} - failed to respond over - {self._poll_retries} - retries and is now suspect for source - {self.host}:{self.port}')

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
            await self._logger.filesystem.aio['hedra.distributed'].debug(f'Node - {check_host}:{check_port} - responded on try - {idx}/{self._poll_interval} - for source - {self.host}:{self.port}')

    def _get_current_term_leader(self):

        current_leader_index = max(
            self._term_number - 1,
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
        host: str,
        port: int
    ):
        # Trigger new election
        self._term_number += 1
        
        self._term_votes[self._term_number][(self.host, self.port)] += 1

        self._election_status = ElectionState.ACTIVE
        self._raft_node_status = NodeState.CANDIDATE


        while len(self._term_leaders) < self._term_number:

            members: List[Tuple[str, int]] = [
                address for address, status in self._node_statuses.items() if status  == 'healthy'
            ]

            members = list(set(members))

            election_timeout = random.uniform(
                self._min_election_timeout,
                self._max_election_timeout
            )

            vote_requests = [
                asyncio.create_task(
                    self.request_vote(
                        member_host,
                        member_port
                    )
                ) for member_host, member_port in members
            ]

            for vote_result in asyncio.as_completed(
                vote_requests, 
                timeout=election_timeout
            ):

                try:
                    response: Tuple[int, RaftMessage] = await vote_result

                    (
                        _, 
                        result
                    ) = response

                    if result.election_status == ElectionState.ACCEPTED:
                        self._term_votes[self._term_number][(self.host, self.port)] += 1

                except asyncio.TimeoutError:
                    pass
        
            quorum_count = int(
                len(members) * (1 - FLEXIBLE_PAXOS_QUORUM) + 1
            )

            if quorum_count <= 1:
                quorum_count = len(members)

            accepted_count = self._term_votes[self._term_number][(self.host, self.port)]

            if accepted_count >= quorum_count:

                # We're the leader!

                self._raft_node_status = NodeState.LEADER
                self._term_leaders.append((self.host, self.port))

                for host, port in members:
                    self._tasks_queue.append(
                        asyncio.create_task(
                            self._update_logs(
                                host,
                                port,
                                [
                                    {
                                        'key': 'election_update',
                                        'value': f'Election complete! Elected - {self.host}:{self.port}'
                                    }
                                ]
                            )
                        )
                    )

            await asyncio.sleep(self._election_poll_interval)

        if self._raft_node_status != NodeState.LEADER:
            self._raft_node_status = NodeState.FOLLOWER

    async def _run_raft_monitor(self):

        while self._running:

            members: List[Tuple[str, int]] = [
                address for address, status in self._node_statuses.items() if status == 'healthy'
            ]

            if self._raft_node_status == NodeState.LEADER:
                for host, port in members:
            
                    self._tasks_queue.append(
                        asyncio.create_task(
                            self._update_logs(
                                host,
                                port,
                                [
                                    {
                                        'key': 'logs_update',
                                        'value': f'Node - {self.host}:{self.port} - submitted log update'
                                    }
                                ]
                            )
                        )
                    )

            else:
                failed_members = list(set([
                    node for node in self.failed_nodes if node not in self.removed_nodes
                ]))

                if len(failed_members) > 0:
                    self._tasks_queue.append(
                        asyncio.create_task(
                            self.run_election()
                        )
                    )

            current_leader_host, current_leader_port = self._get_current_term_leader()

            await self._logger.distributed.aio.debug(f'Source - {self.host}:{self.port} - has node {current_leader_host}:{current_leader_port} - as leader for term - {self._term_number}')
            await self._logger.filesystem.aio['hedra.distributed'].info(f'Source - {self.host}:{self.port} - has node {current_leader_host}:{current_leader_port} - as leader for term - {self._term_number}')

            await asyncio.sleep(
                self._logs_update_poll_interval
            )

    async def _cleanup_pending_raft_tasks(self):

        await self._logger.distributed.aio.debug(f'Running cleanup for source - {self.host}:{self.port}')
        await self._logger.filesystem.aio['hedra.distributed'].debug(f'Running cleanup for source - {self.host}:{self.port}')

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
            await self._logger.filesystem.aio['hedra.distributed'].debug(f'Cleaned up - {pending_count} - for source - {self.host}:{self.port}')

            await asyncio.sleep(self._logs_update_poll_interval)

    async def leave(self):
        await self._logger.distributed.aio.debug(f'Shutdown requested for RAFT source - {self.host}:{self.port}')
        await self._logger.filesystem.aio['hedra.distributed'].debug(f'Shutdown requested for RAFT source - {self.host}:{self.port}')

        await cancel(self._raft_monitor_task)
        await cancel(self._raft_cleanup_task)

        await self._submit_leave_requests()
        await self._shutdown()

        await self._logger.distributed.aio.debug(f'Shutdown complete for RAFT source - {self.host}:{self.port}')
        await self._logger.filesystem.aio['hedra.distributed'].debug(f'Shutdown complete for RAFT source - {self.host}:{self.port}')
