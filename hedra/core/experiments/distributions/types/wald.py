from typing import Union
from scipy.stats import wald
from .base import BaseDistribution


class WaldDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        center: Union[int, float]=0.1,
        randomness: Union[int, float]=1
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=wald(
                loc=center,
                scale=randomness
            )
        )
