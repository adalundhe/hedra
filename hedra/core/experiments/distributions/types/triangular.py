from typing import Union
from scipy.stats import triang
from .base import BaseDistribution


class TriangularDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: float=0.25,
        beta: float=0.5,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=triang(
                alpha,
                beta,
                loc=center,
                scale=randomness
            )
        )
