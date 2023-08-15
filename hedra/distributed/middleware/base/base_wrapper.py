from typing import Callable, Coroutine, Any


class BaseWrapper:
    
    def __init__(self) -> None:
        self.setup: Callable[
            [],
            Coroutine[
                Any,
                Any,
                None
            ]
        ] = None