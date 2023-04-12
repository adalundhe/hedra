from typing import Union
from scipy.stats import halfgennorm
from .base import BaseDistribution


class HalfGeneralizedNormalDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        beta: float=0.5,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=halfgennorm(
                beta,
                loc=center,
                scale=randomness
            )
        )
