from typing import Union
from scipy.stats import kappa4
from .base import BaseDistribution


class Kappa4Distribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: Union[int, float]=1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=kappa4(
                alpha,
                loc=center,
                scale=randomness
            )
        )
