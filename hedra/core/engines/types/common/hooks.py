from typing import Coroutine, List



class Hooks:

    def __init__(
        self,
        before: Coroutine = None,
        after: Coroutine = None,
        checks: List[Coroutine] = []
    ) -> None:
        self.before = before
        self.after = after
        self.checks = checks