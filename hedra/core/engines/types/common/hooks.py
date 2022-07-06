from typing import Coroutine


class Hooks:

    def __init__(
        self,
        before: Coroutine = None,
        after: Coroutine = None,
        before_batch: Coroutine = None,
        after_batch: Coroutine = None
    ) -> None:
        self.before = before
        self.after = after
        self.before_batch = before_batch
        self.after_batch = after_batch