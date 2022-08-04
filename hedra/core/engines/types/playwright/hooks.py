from typing import Coroutine, List


class Hooks:

    def __init__(self) -> None:
        self.before: Coroutine = None
        self.after: Coroutine = None
        self.checks: List[Coroutine] = []