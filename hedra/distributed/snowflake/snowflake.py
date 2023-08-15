from dataclasses import dataclass
from datetime import (
    datetime,
    tzinfo,
    timedelta
)
from typing import Optional
from .constants import (
    MAX_INSTANCE,
    MAX_SEQ
)


@dataclass(frozen=True)
class Snowflake:
    timestamp: int
    instance: int
    epoch: int = 0
    seq: int = 0


    @classmethod
    def parse(cls, snowflake: int, epoch: int = 0) -> 'Snowflake':
        return cls(
            epoch=epoch,
            timestamp=snowflake >> 22,
            instance=snowflake >> 12 & MAX_INSTANCE,
            seq=snowflake & MAX_SEQ
        )

    @property
    def milliseconds(self) -> int:
        return self.timestamp + self.epoch

    @property
    def seconds(self) -> float:
        return self.milliseconds / 1000

    @property
    def datetime(self) -> datetime:
        return datetime.utcfromtimestamp(self.seconds)

    def datetime_tz(self, tz: Optional[tzinfo] = None) -> datetime:
        return datetime.fromtimestamp(self.seconds, tz=tz)

    @property
    def timedelta(self) -> timedelta:
        return timedelta(milliseconds=self.epoch)

    @property
    def value(self) -> int:
        return self.timestamp << 22 | self.instance << 12 | self.seq

    def __int__(self) -> int:
        return self.value