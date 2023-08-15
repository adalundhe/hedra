import asyncio
import random
from collections import (
    defaultdict,
    deque,
    OrderedDict
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
from hedra.distributed.models.raft.election_state import ElectionState
from hedra.distributed.models.raft.logs import Entry, NodeState
from hedra.distributed.monitoring import Monitor
from hedra.distributed.service.controller import Controller
from hedra.distributed.snowflake.snowflake_generator import (
    SnowflakeGenerator,
    Snowflake
)
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
from .errors import InvalidTermError
from .log_queue import LogQueue


class RaftManager(Controller[Monitor]):

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
            env = load_env(Env)

        raft_env = load_env(RaftEnv) 

        super().__init__(
            host,
            port,
            cert_path=cert_path,
            key_path=key_path,
            workers=workers,
            env=env,
            engine=engine,
            plugins={
                'monitor': Monitor
            }
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
        self._term_leaders: List[Tuple[str, int]] = [
            (self.host, self.port)
        ]
        self._running = False

        self._logs: Dict[int, Dict[int, Any]] = LogQueue()
        self._previous_entry_index = 0
        self._last_timestamp = 0
        self._last_commit_timestamp = 0
        self._term_number = 0
        self._tasks_queue: Deque[asyncio.Task] = deque()
        self._entry_id_generator = SnowflakeGenerator(self._instance_id)
        
    async def start(
        self,
        cert_path: Optional[str]=None,
        key_path: Optional[str]=None
    ):

        self._running = True

        loop = asyncio.get_event_loop()
        self._last_commit_timestamp = loop.time()

        await self.start_server(
            cert_path=cert_path,
            key_path=key_path
        )
        
        await asyncio.gather(*[
            monitor.start(   
                cert_path=cert_path,
                key_path=key_path
            ) for monitor in self._plugins['monitor'].each()
        ])

        self._timeout = random.uniform(
            self._min_election_timeout,
            self._max_election_timeout
        )

    async def register(
        self,
        host: str,
        port: int         
    ):
        await asyncio.gather(*[
            monitor.register(
                host,
                port
            ) for monitor in self._plugins['monitor'].each()
        ])

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

        if self._election_status in [ElectionState.ACTIVE, ElectionState.PENDING] and term_number > self._term_number:
            # The requesting node is ahead. They're elected the leader by default.
            elected_host = source_host
            elected_port = source_port
            
        elif term_number == self._term_number:
            # The rterm numbers match, we can elect a candidate.

            members: List[Tuple[str, int]] = []

            for monitor in self._plugins['monitor'].each():
                members.extend([
                    address for address, status in monitor._node_statuses.items() if status  == 'healthy'
                ])

            elected_host, elected_port = random.choice(
                list(set(members))
            )
        
        if elected_host == source_host and elected_port == source_port:
            
            election_result = RaftMessage(
                host=source_host,
                port=source_port,
                election_status="ACCEPTED",
                term_number=term_number
            )
            
            self._raft_node_status = NodeState.FOLLOWER
        
        else:

            election_result = RaftMessage(
                host=source_host,
                port=source_port,
                election_status="REJECTED",
                term_number=term_number
            )

        return election_result
    
    @server()
    async def receive_log_update(
        self,
        shard_id: int,
        message: RaftMessage
    ):
        entries_count = len(message.entries)

        if entries_count < 0:
            return RaftMessage(
                host=message.host,
                port=message.port,
                source_host=self.host,
                source_port=self.port,
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

        error = self._logs.update(entries)

        if isinstance(error, Exception):

            elected_leader = self._term_leaders[-1]

            return RaftMessage(
                host=message.host,
                port=message.port,
                source_host=self.host,
                source_port=self.port,
                election_status=self._election_status,
                raft_node_status=self._raft_node_status,
                error=str(error),
                elected_leader=elected_leader,
                updated_term=self._term_number
            )

        return RaftMessage(
            host=message.host,
            port=message.port,
            source_host=self.host,
            source_port=self.port,
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
        
        return RaftMessage(
            host=host,
            port=port,
            source_host=self.host,
            source_port=self.port,
            election_status=self._election_status,
            raft_node_status=self._raft_node_status,
            entries=[
                Entry(
                    entry_id=self._entry_id_generator.generate(),
                    term=self._term_number,
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
        
        _, response = await self.submit_log_update(
            host,
            port,
            entries
        )

        elected_leader = self._term_leaders[-1]

        if response.error and elected_leader != response.elected_leader:
            self._term_leaders.append(response.elected_leader)
            self._term_number = response.updated_term

    async def _start_suspect_monitor(self):

        monitor = self._plugins['monitor'].one

        (
            suspect_host,
            suspect_port,
            status
        ) = await monitor._start_suspect_monitor()


        if status == 'failed':
            # Trigger new election
            self._term_number += 1
            
            self._term_votes[self._term_number][(self.host, self.port)] += 1

            members: List[Tuple[str, int]] = []

            for monitor in self._plugins['monitor'].each():
                members.extend([
                    address for address, status in monitor._node_statuses.items() if status  == 'healthy'
                ])

            members = list(set(members))

            loop = asyncio.get_event_loop()

            elapsed = 0
            start = loop.time()

            election_timeout = random.uniform(
                self._min_election_timeout,
                self._max_election_timeout
            )

            vote_requests = [
                asyncio.create_task(
                    self.request_vote(
                        member_host,
                        member_port,
                        suspect_host,
                        suspect_port
                    )
                ) for member_host, member_port in members
            ]

            accepted_count = 0

            for vote_result in asyncio.as_completed(
                vote_requests, 
                timeout=election_timeout
            ):

                try:
                    (
                        _, 
                        result
                    ): Tuple[int, RaftMessage] = await vote_result

                    if result.election_status == ElectionState.ACCEPTED:
                        accepted_count += 1

                except asyncio.TimeoutError:
                    pass
        
            quorum_count = int(
                len(members) * (1 - FLEXIBLE_PAXOS_QUORUM) + 1
            )

            if accepted_count >= quorum_count:
                self._raft_node_status = NodeState.LEADER

    async def start_raft_monitor(self):

        while self._running:

            members: List[Tuple[str, int]] = []

            for monitor in self._plugins['monitor'].each():
                members.extend([
                    address for address, status in monitor._node_statuses.items() if status == 'healthy'
                ])

            members = list(set(members))

            if self._raft_node_status == NodeState.LEADER:
                for host, port in members:
            
                    self._tasks_queue.append(
                        asyncio.create_task(
                            self.submit_log_update(
                                host,
                                port
                            )
                        )
                    )

            await asyncio.sleep(
                self._logs_update_poll_interval
            )
