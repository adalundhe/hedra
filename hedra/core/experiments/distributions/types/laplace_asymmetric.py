from typing import Union
from scipy.stats import laplace_asymmetric
from .base import BaseDistribution


class LaplaceAsymmetricDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: Union[int, float]=0.5,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=laplace_asymmetric(
                alpha,
                loc=center,
                scale=randomness
            )
        )
