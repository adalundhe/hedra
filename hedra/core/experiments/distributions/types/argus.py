from typing import Union
from scipy.stats import argus
from .base import BaseDistribution


class ArgusDistribution(BaseDistribution):

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
            frozen_distribution=argus(
                1,
            center=center,
            randomness=randomness,
            )
        )