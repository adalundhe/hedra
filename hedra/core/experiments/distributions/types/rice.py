from typing import Union
from scipy.stats import rice
from .base import BaseDistribution


class RiceDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        mu: Union[int, float]=0.5,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=rice(
                mu,
                loc=center,
                scale=randomness
            )
        )
