from typing import Optional, List
from hedra.versioning.flags.types.unstable.flag import unstable
from .distribution import Distribution


@unstable
class Variant:

    def __init__(
        self,
        stage_name: str,
        weight: Optional[float] = None,
        distribution: str=None,
        distribution_intervals: int=10,
        optimize_from: List[str]=[]
    ) -> None:
        self.stage_name = stage_name
        self.weight = weight
        self.distribution: Optional[Distribution] = None
        self.intervals = distribution_intervals
        self.optimize_from: List[str] = optimize_from
        
        if distribution:
            self.distribution = Distribution(
                distribution,
                intervals=distribution_intervals
            )
