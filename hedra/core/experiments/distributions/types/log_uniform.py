from typing import Union
from scipy.stats import loguniform
from .base import BaseDistribution


class LogUniformDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: Union[int, float]=0.1,
        beta: Union[int, float]=0.25,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=loguniform(
                alpha,
                beta,
                loc=center,
                scale=randomness
            )
        )
