import time
from hedra.distributed.env import (
    load_env,
    ReplicationEnv
)
from hedra.distributed.env.time_parser import TimeParser
from hedra.distributed.models.raft.logs import Entry
from hedra.distributed.snowflake.snowflake_generator import Snowflake
from typing import List, Dict, Union
from .errors import InvalidTermError


class LogQueue:

    def __init__(self) -> None:

        env = load_env(ReplicationEnv)

        self.logs: List[Entry] = []
        self._timestamps: List[float] = []
        self._commits: List[float] = []
        self.timestamp_index_map: Dict[float, int] = {}
        self._term = 0
        self.size = 0
        self.commit_index = 0
        self._last_timestamp = 0
        self._last_commit_timestamp = 0
        self._prune_max_age = TimeParser(
            env.MERCURY_SYNC_RAFT_LOGS_PRUNE_MAX_AGE
        ).time
        self._prune_max_count = env.MERCURY_SYNC_RAFT_LOGS_PRUNE_MAX_COUNT

    @property
    def last_timestamp(self):

        if len(self._timestamps) > 0:
            return self._timestamps[-1]
        
        else:
            return 0

    def latest(self):

        if len(self._commits) > 0:
            latest_commit_timestamp = self._commits[-1]
            latest_index = self.timestamp_index_map[latest_commit_timestamp]

        else:
            latest_index = 0

        return self.logs[latest_index:]
    
    def commit(self):

        if len(self._timestamps) > 0:
            self._last_commit_timestamp = self._timestamps[-1]
            self._commits.append(self._last_commit_timestamp)

    def get(self, shard_id: int):
        flake = Snowflake.parse(shard_id)

        index = self.timestamp_index_map.get(flake.timestamp, -1)

        if self.size < 1:
            return None

        return self.logs[index]
    
    def filter(self, key: str):
        return [
            entry for entry in self.logs if entry.key == key
        ]
    
    def update(
        self, 
        entries: List[Entry]
    ) -> Union[Exception, None]:

        last_entry = entries[-1]
        
        last_entry_id = Snowflake.parse(last_entry.entry_id)
        last_entry_term = last_entry.term

        if last_entry_term < self._term:
            return InvalidTermError(
                last_entry_id,
                last_entry_term,
                self._term
            )
        
        # Did we miss an election or havent caught on to a leader change? let's update!
        elif last_entry_term > self._term:
            self._term = last_entry_term

        if self.size < 1:

            for idx, entry in enumerate(entries):

                entry_id = Snowflake.parse(entry.entry_id)
                entry_timestamp = entry_id.timestamp

                self.timestamp_index_map[entry_timestamp] = idx
                self._timestamps.append(entry_timestamp)
                self.logs.append(entry)

                self.size += 1

        else:

            for entry in entries:

                if len(self._timestamps) > 0:
                    last_queue_timestamp = self._timestamps[-1]  

                else:
                    last_queue_timestamp = 0

                next_index = self.size

                entry_id = Snowflake.parse(entry.entry_id)
                entry_timestamp = entry_id.timestamp

                # We've received a missing entry so insert it in order..
                if entry_timestamp < last_queue_timestamp:

                    # The insert index is at the index of last timestamp less 
                    # than the entry timestamp + 1.
                    #
                    # I.e. if the last idx < timestamp is 4 we insert at 5.
                    #

                    previous_timestamps = [
                        idx for idx, timestamp in enumerate(self._timestamps) if timestamp < entry_timestamp
                    ]

                    if len(previous_timestamps) > 0:
                        
                        last_previous_timestamp_idx = previous_timestamps[-1]

                        insert_index: int = last_previous_timestamp_idx + 1

                        next_logs = self.logs[insert_index:]
                        next_timestamps = self._timestamps[insert_index:]

                        previous_logs = self.logs[:insert_index]
                        previous_timestamps = self._timestamps[:insert_index]

                    else:
                        
                        insert_index = 0

                        next_logs = self.logs
                        next_timestamps = self._timestamps

                        previous_logs = []
                        previous_timestamps = []
                
                    previous_logs.append(entry)
                    previous_timestamps.append(entry_timestamp)

                    previous_logs.extend(next_logs)
                    previous_timestamps.extend(next_timestamps)

                    self.timestamp_index_map[entry_timestamp] = insert_index

                    for timestamp in next_timestamps:
                        self.timestamp_index_map[timestamp] += 1

                    self.logs = previous_logs
                    self._timestamps = previous_timestamps

                    self.size += 1
                
                # We've received entries to append
                elif entry_timestamp > last_queue_timestamp:
                        
                    self.logs.append(entry)
                    self._timestamps.append(entry_timestamp)
                
                    self.timestamp_index_map[entry_timestamp] = next_index
                    self.size += 1

                # We've receive an entry to replace.
                else:
                    
                    next_index = self.timestamp_index_map[entry_timestamp]

                    self.logs[next_index] = entry   
                    self._timestamps[next_index] = entry_timestamp
            

    def prune(self):

        current_time = int(time.time() * 1000)
        
        # Get the number of timestamps older than our max prune age
        count = len([
            timestamp for timestamp in self._timestamps if current_time - timestamp > self._prune_max_age
        ])

        # If greater than our max prune count, set prune count as max prune count.
        if count > self._prune_max_count:
            count = self._prune_max_count

        if count >= self.size:

            self.logs = []
            self._timestamps = []
            self.timestamp_index_map = {}
            self._commits = []

            self.size = 0
            self.commit_index = 0
            self._last_timestamp = 0
            self._last_commit_timestamp = 0
            self.size = 0

        else:

            pruned_timestamps = self._timestamps[:count]

            for timestamp in pruned_timestamps:
                if self.timestamp_index_map.get(timestamp):
                    del self.timestamp_index_map[timestamp]

            self.logs = self.logs[count:]
            self._timestamps = self._timestamps[count:]

            self._commits = [
                commit for commit in self._commits if commit > self._timestamps[0]
            ]

            self.size -= count
            

        

