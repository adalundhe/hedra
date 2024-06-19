from _collections_abc import (
    dict_items,
    dict_keys,
    dict_values,
)
from typing import (
    Any,
    Dict,
    Generator,
    Optional,
)

from hedra.core_rewrite.hooks.optimized.models.base import FrozenDict, OptimizedArg

from .headers_validator import HeaderValidator


class Headers(OptimizedArg):
    def __init__(self, headers: Dict[str, str | int | float | bool]) -> None:
        super(
            OptimizedArg,
            self,
        ).__init__()

        validated_headers = HeaderValidator(headers=headers)
        self.data: FrozenDict = FrozenDict(validated_headers.value)
        self.optimized: Optional[str] = None

    def __getitem__(self, key: str) -> str | int | float | bool:
        return self.data[key]

    def __iter__(self) -> Generator[str, Any, None]:
        for key in self.data:
            yield key

    def items(self) -> dict_items[str, str | int | float | bool]:
        return self.data.items()

    def keys(self) -> dict_keys[str]:
        return self.data.keys()

    def values(self) -> dict_values[str | int | float | bool]:
        return self.data.values()

    def get(
        self,
        key: str,
        default: Optional[str | int | float | bool] = None,
    ) -> Optional[str | int | float | bool]:
        return self.data.get(key, default)
