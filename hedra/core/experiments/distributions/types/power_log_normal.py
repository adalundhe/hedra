from typing import Union
from scipy.stats import powerlognorm
from .base import BaseDistribution


class PowerLogNormalDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        c_value: Union[int, float]=0.5,
        s_value: Union[int, float]=1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=powerlognorm(
                c_value,
                s_value,
                loc=center,
                scale=randomness
            )
        )
