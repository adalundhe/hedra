from typing import Union
from scipy.stats import levy_stable
from .base import BaseDistribution


class LevyStableDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: Union[int,float]=1,
        beta: Union[int,float]=0.5,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=levy_stable(
                alpha,
                beta,
                loc=center,
                scale=randomness
            )
        )
