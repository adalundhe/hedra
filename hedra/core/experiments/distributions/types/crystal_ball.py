from typing import Union
from scipy.stats import crystalball
from .base import BaseDistribution


class CrystalDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: int=5,
        beta: int=6,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=crystalball(
                alpha,
                beta,
                loc=center,
                scale=randomness
            )
        )
