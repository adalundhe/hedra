from typing import Union
from scipy.stats import trapezoid
from .base import BaseDistribution


class TrapezoidDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: float=0.25,
        beta: float=0.5,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=trapezoid(
                alpha,
                beta,
                loc=center,
                scale=randomness
            )
        )
