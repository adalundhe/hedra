from typing import Union
from scipy.stats import chi2
from .base import BaseDistribution


class ChiSquaredDistribution(BaseDistribution):

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
            frozen_distribution=chi2(
                distribution_function,
                loc=center,
                scale=randomness
            )
        )
