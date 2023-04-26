from typing import Union
from scipy.stats import truncnorm
from .base import BaseDistribution


class TruncatedNormalDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: float=1,
        center: Union[int, float]=1,
        randomness: Union[int, float]=1
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=truncnorm(
                alpha,
                loc=center,
                scale=randomness
            )
        )
