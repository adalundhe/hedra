from typing import Any


class SimpleContext:

    def __init__(self, **kwargs) -> None:
        for kwarg_name, kwarg in kwargs.items():
            object.__setattr__(self, kwarg_name, kwarg)

    def __getattribute__(self, __name: str) -> Any:
        return object.__getattribute__(self, __name)

    def __setattr__(self, __name: str, __value: Any) -> None:
        object.__setattr__(self, __name, __value)