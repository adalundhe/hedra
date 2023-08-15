from enum import Enum

class QueryType(Enum):
    REQUEST=0
    RESPONSE=1

    @classmethod
    def by_value(cls, value: int):
        value_map = {
            0: QueryType.REQUEST,
            1: QueryType.RESPONSE
        }

        return value_map.get(
            value,
            QueryType.REQUEST
        )
