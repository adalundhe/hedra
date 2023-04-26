from typing import Union
from scipy.stats import dweibull
from .base import BaseDistribution


class DWeibullDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: Union[int, float]=1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=dweibull(
                alpha,
                loc=center,
                scale=randomness
            )
        )
