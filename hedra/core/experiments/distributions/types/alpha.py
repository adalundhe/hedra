from typing import Union
from scipy.stats import alpha as alpha_dist
from .base import BaseDistribution


class AlphaDistribution(BaseDistribution):

    def __init__(
        self, 
        size: int,
        alpha: Union[int, float]=1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ) -> None:
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=alpha_dist(
                alpha,
                loc=center,
                scale=randomness
            )
        )