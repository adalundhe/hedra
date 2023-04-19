from typing import Union
from scipy.stats import weibull_max
from .base import BaseDistribution


class WeibullMaxmimumDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        a_value: float=0.5,
        center: Union[int, float]=0.1,
        randomness: Union[int, float]=1
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=weibull_max(
                a_value,
                loc=center,
                scale=randomness
            )
        )
