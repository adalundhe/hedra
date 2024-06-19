from typing import Optional

from hedra.core_rewrite.hooks.optimized.models.base import OptimizedArg

from .query_validator import QueryValidator


class Query(OptimizedArg):
    def __init__(self, query: str) -> None:
        super(
            OptimizedArg,
            self,
        ).__init__()

        validated_query = QueryValidator(value=query)
        self.data = validated_query.value
        self.optimized: Optional[str] = None

    def __str__(self) -> str:
        return self.data

    def __repr__(self) -> str:
        return self.data
