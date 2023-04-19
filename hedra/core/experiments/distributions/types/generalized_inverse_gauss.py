import random
from typing import Union
from scipy.stats import gibrat
from .base import BaseDistribution


class GeneralizedInverseGaussDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        a_value: float=0.5,
        b_value: Union[int, float]=random.uniform(0, 1),
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=gibrat(
                a_value,
                b_value,
                loc=center,
                scale=randomness
            )
        )
