from typing import Union
from scipy.stats import gamma
from .base import BaseDistribution


class GammaDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: Union[int, float]=2,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=gamma(
                alpha,
                loc=center,
                scale=randomness
            )
        )
