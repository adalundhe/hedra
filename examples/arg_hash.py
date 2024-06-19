import threading
import uuid
from typing import Optional

from hedra.core_rewrite.snowflake.snowflake_generator import SnowflakeGenerator


class OptimizedArg:
    def __init__(self) -> None:
        self._snowflake = SnowflakeGenerator(
            (uuid.uuid1().int + threading.get_native_id()) >> 64
        )

        self.arg_id = self._snowflake.generate()
        self.call_id: Optional[int] = None

        self.optimized: bool = False

    def __hash__(self):
        return self.arg_id

    def __eq__(self, value: object) -> bool:
        return (
            isinstance(
                value,
                OptimizedArg,
            )
            and value.arg_id == self.arg_id
        )


arg = OptimizedArg()

test = {}

test[arg] = arg

print(test)
