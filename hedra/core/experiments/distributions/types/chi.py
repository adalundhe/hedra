from typing import Union
from scipy.stats import chi
from .base import BaseDistribution


class ChiDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        distribution_function: Union[int, float]=1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=chi(
                distribution_function,
                loc=center,
                scale=randomness
            )
        )
