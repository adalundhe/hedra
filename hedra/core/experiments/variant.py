from typing import Optional, List, Union
from hedra.versioning.flags.types.unstable.flag import unstable
from .distribution import Distribution
from .mutations.types.base.mutation import Mutation


@unstable
class Variant:

    def __init__(
        self,
        stage_name: str,
        weight: Optional[float] = None,
        distribution: str=None,
        distribution_intervals: int=10,
        mutations: Optional[List[Mutation]]=None
    ) -> None:
        self.stage_name = stage_name
        self.weight = weight
        self.distribution: Optional[Distribution] = None
        self.intervals = distribution_intervals
        self.mutations: Union[List[Mutation], None] = mutations
        
        if distribution:
            self.distribution = Distribution(
                distribution,
                intervals=distribution_intervals
            )

    def get_mutations(self) -> List[Mutation]:
        return [
            mutation.copy() for mutation in self.mutations
        ]
