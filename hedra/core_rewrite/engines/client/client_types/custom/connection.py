from typing import Any, Coroutine


class CustomConnection:

    __slots__ = (
        "security_context",
        "reset_connection",
        "connected",
        "pending",
        "make_connection",
        "execute",
        "close",
    )

    def __init__(
        self, 
        security_context: Any, 
        reset_connection: bool=False,
        on_connect: Coroutine=None,
        on_execute: Coroutine=None,
        on_close: Coroutine=None
    ) -> None:
        self.security_context = security_context
        self.reset_connection = reset_connection
        self.connected = False
        self.pending = 0
        
        self.make_connection = on_connect
        self.execute = on_execute
        self.close = on_close
