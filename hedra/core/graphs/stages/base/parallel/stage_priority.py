from __future__ import annotations
import math
from enum import Enum
from typing import Dict, Tuple, Union


class StagePriority(Enum):
    LOW=1
    NORMAL=2
    HIGH=3
    EXCLUSIVE=4
    AUTO=5

    

    @classmethod
    def map(cls, priority: Union[str, None]) -> StagePriority:
        priority_map: Dict[str, StagePriority] = {
            'low': cls.LOW,
            'normal': cls.NORMAL,
            'high': cls.HIGH,
            'exclusive': cls.EXCLUSIVE,
            'auto': cls.AUTO
        }

        if priority is None:
            return StagePriority.AUTO

        return priority_map.get(
            priority,
            StagePriority.AUTO
        )
    
    @classmethod
    def get_worker_allocation_range(
        cls, 
        priority_type: StagePriority,
        pool_size: int
    ) -> Tuple[int, int]:
            

        weight_map: Dict[StagePriority, Tuple[int, int]] = {
            StagePriority.LOW: (
                1, 
                math.ceil(pool_size * 0.25)
            ),
            StagePriority.NORMAL: (
                math.ceil(pool_size * 0.25),
                math.ceil(pool_size * 0.75)
            ),
            StagePriority.HIGH: (
                math.ceil(pool_size * 0.75),
                pool_size
            ),
            StagePriority.EXCLUSIVE: (
                pool_size,
                pool_size,
            ),
            StagePriority.AUTO: (
                1,
                pool_size
            )
        }

        return weight_map.get(priority_type)
    


