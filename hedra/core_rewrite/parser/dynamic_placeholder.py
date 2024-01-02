from typing import Any


class DynamicPlaceholder:

    def __init__(self, name: str) -> None:
        self.name = name
        self.value: Any = None