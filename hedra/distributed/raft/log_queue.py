from hedra.distributed.models.raft.logs import Entry
from hedra.distributed.snowflake.snowflake_generator import Snowflake
from typing import List, Tuple, Union
from .errors import InvalidTermError


class LogQueue:

    def __init__(self) -> None:
        self.logs: List[Entry] = []
        self._timestamps: List[int] = []
        self.timestamp_index_map = {}
        self.term = 0
        self.commit_index = 0

    @property
    def size(self):
        return len(self.logs)

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

                self.size += 1

            self.logs = entries

        else:

            last_queue_timestamp = self._timestamps[-1]  

            for entry in entries:

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

                    insert_index: int = [
                        idx for idx, timestamp in enumerate(self._timestamps) if timestamp < entry_timestamp
                    ].pop() + 1

                    next_logs = self.logs[insert_index:]
                    next_timestamps = self._timestamps[insert_index:]

                    previous_logs = self.logs[:insert_index - 1]
                    previous_timestamps = self._timestamps[:insert_index - 1]
                    
                    self.timestamp_index_map[entry_timestamp] = insert_index
                
                    previous_logs.append(entry)
                    previous_timestamps.append(entry_timestamp)

                    previous_logs.extend(next_logs)
                    previous_timestamps.extend(next_timestamps)

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
                    next_index += 1
                
                    self.size += 1

                else:
                    
                    next_index = self.timestamp_index_map[entry_timestamp]
                    self.logs[next_index] = entry            
