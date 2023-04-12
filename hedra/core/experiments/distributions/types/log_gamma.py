from typing import Union
from scipy.stats import loggamma
from .base import BaseDistribution


class LogGammaDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        c_value: Union[int, float]=0.1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=loggamma(
                c_value,
                loc=center,
                scale=randomness
            )
        )
