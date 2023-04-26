import random
from typing import Union
from scipy.stats import rdist
from .base import BaseDistribution


class RDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        c_value: Union[int, float]=random.randint(1, 10),
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=rdist(
                c_value,
                loc=center,
                scale=randomness
            )
        )
