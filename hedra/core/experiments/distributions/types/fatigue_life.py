from typing import Union
from scipy.stats import fatiguelife
from .base import BaseDistribution


class FatigueLifeDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: Union[int, float]=0.5,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25

    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=fatiguelife(
                alpha,
                loc=center,
                scale=randomness
            )
        )
