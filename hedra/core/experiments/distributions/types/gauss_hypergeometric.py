import random
from typing import Union
from scipy.stats import gausshyper
from .base import BaseDistribution


class GaussHypergeometricDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        a_value: float=random.uniform(0, 1),
        b_value: float=random.uniform(0, 1),
        c_value: float=random.uniform(0, 1),
        z_value: float=random.uniform(0, 1),
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=gausshyper(
                a_value,
                b_value,
                c_value,
                z_value,
                loc=center,
                scale=randomness
            )
        )
