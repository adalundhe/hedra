from typing import Union
from scipy.stats import weibull_min
from .base import BaseDistribution


class WeibullMinimumDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        a_value: float=0.25,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.1
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=weibull_min(
                a_value,
                loc=center,
                scale=randomness
            )
        )
