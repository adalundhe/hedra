from time import time
from typing import Optional
from .constants import (
    MAX_SEQ
)
from .snowflake import Snowflake


class SnowflakeGenerator:
    def __init__(
        self, 
        instance: int, 
        *, 
        seq: int = 0, 
        timestamp: Optional[int] = None
    ):

        current = int(time() * 1000)


        timestamp = timestamp or current

        self._ts = timestamp

        self._inf = instance << 12
        self._seq = seq

    @classmethod
    def from_snowflake(cls, sf: Snowflake) -> 'SnowflakeGenerator':
        return cls(sf.instance, seq=sf.seq, epoch=sf.epoch, timestamp=sf.timestamp)

    def __iter__(self):
        return self

    def generate(self) -> Optional[int]:

        current = int(time() * 1000)

        if self._ts == current:

            if self._seq == MAX_SEQ:
                return None
            
            self._seq += 1

        elif self._ts > current:
            return None
        
        else:
            self._seq = 0

        self._ts = current

        return self._ts << 22 | self._inf | self._seq