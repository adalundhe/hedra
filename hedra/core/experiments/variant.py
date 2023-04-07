from typing import Optional
from hedra.versioning.flags.types.unstable.flag import unstable
from .distribution import Distribution


@unstable
class Variant:

    def __init__(
        self,
        stage_name: str,
        weight: Optional[float] = None,
        distribution: str=None,
        distribution_intervals: int=10
    ) -> None:
        self.stage_name = stage_name
        self.weight = weight
        self.distribution: Optional[Distribution] = None
        
        if distribution:
            self.distribution = Distribution(
                distribution,
                intervals=distribution_intervals
            )
