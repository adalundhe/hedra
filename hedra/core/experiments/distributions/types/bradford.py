from typing import Union
from scipy.stats import bradford
from .base import BaseDistribution


class BradfordDistribution(BaseDistribution):

    def __init__(
        self, 
        size: int,
        alpha: float=0.5,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            loc=center,
            scale=randomness,
            frozen_distribution=bradford(
                alpha,
                loc=center,
                scale=randomness
            )
        )