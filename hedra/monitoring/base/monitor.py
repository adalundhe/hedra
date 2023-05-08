from collections import defaultdict
from typing import Dict, List


class BaseMonitor:

    def __init__(self) -> None:
        self.active: Dict[str, int] = {}
        self.collected: Dict[str, List[int]] = defaultdict(list)