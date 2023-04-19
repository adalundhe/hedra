from typing import Union
from scipy.stats import truncweibull_min
from .base import BaseDistribution


class TruncatedWeibullMinimumDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        a_value: float=2,
        b_value: float=0.5,
        c_value: float=1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=1
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=truncweibull_min(
                a_value,
                b_value,
                c_value,
                loc=center,
                scale=randomness
            )
        )
