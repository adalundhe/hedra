from typing import Union
from scipy.stats import t
from .base import BaseDistribution


class TDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: float=0.5,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=t(
                alpha,
                loc=center,
                scale=randomness
            )
        )
