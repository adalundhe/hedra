from typing import Coroutine



class Hooks:

    def __init__(
        self,
        before: Coroutine = None,
        after: Coroutine = None,
        checks: Coroutine = []
    ) -> None:
        self.before = before
        self.after = after
        self.checks = checks