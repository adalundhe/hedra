from typing import Union
from scipy.stats import laplace
from .base import BaseDistribution


class LaplaceDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=laplace(
                loc=center,
                scale=randomness
            )
        )
