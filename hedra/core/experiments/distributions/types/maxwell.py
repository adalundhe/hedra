from typing import Union
from scipy.stats import maxwell
from .base import BaseDistribution


class MaxwellDistribution(BaseDistribution):

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
            frozen_distribution=maxwell(
                loc=center,
                scale=randomness
            )
        )
