from typing import Union
from scipy.stats import studentized_range
from .base import BaseDistribution


class StudentRangeDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: float=3,
        beta: float=10,
        center: Union[int, float]=0.1,
        randomness: Union[int, float]=0.2
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=studentized_range(
                alpha,
                beta,
                loc=center,
                scale=randomness
            )
        )
