from typing import Union
from scipy.stats import fisk
from .base import BaseDistribution


class FiskDistribution(BaseDistribution):

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
            frozen_distribution=fisk(
                alpha,
                loc=center,
                scale=randomness
            )
        )
