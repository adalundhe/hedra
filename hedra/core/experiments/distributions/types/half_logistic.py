from typing import Union
from scipy.stats import halflogistic
from .base import BaseDistribution


class HalfLogisticDistribution(BaseDistribution):

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
            frozen_distribution=halflogistic(
                loc=center,
                scale=randomness
            )
        )
