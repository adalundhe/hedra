import random
from typing import Union
from scipy.stats import powerlaw
from .base import BaseDistribution


class PowerlawDistribution(BaseDistribution):

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
            frozen_distribution=powerlaw(
                c_value,
                loc=center,
                scale=randomness
            )
        )
