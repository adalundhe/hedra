import random
from typing import Union
from scipy.stats import genhyperbolic
from .base import BaseDistribution


class GeneralizedHyperbolicDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        a_value: float=0.5,
        b_value: float=random.uniform(1, 10),
        c_value: float=random.uniform(-1, -0.1),
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=genhyperbolic(
                a_value,
                b_value,
                c_value,
                loc=center,
                scale=randomness
            )
        )
