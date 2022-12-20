from typing import List


class HookMetadata:

    def __init__(
        self, 
        weight: int = 1, 
        order: int = 1, 
        env: str = None, 
        user: str = None, 
        tags: List[str] = []
    ) -> None:
        self.weight = weight
        self.order = order
        self.env = env
        self.user = user
        self.tags = tags