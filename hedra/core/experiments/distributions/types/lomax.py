from typing import Union
from scipy.stats import lomax
from .base import BaseDistribution


class LomaxDistribution(BaseDistribution):

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
            frozen_distribution=lomax(
                alpha,
                loc=center,
                scale=randomness
            )
        )
