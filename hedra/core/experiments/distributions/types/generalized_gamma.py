import random
from typing import Union
from scipy.stats import gengamma
from .base import BaseDistribution


class GeneralizedGammaDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        a_value: float=0.5,
        c_value: float=random.uniform(0, 1),
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=gengamma(
                a_value,
                c_value,
                loc=center,
                scale=randomness
            )
        )
