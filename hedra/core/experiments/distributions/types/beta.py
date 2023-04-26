from typing import Union
from scipy.stats import beta
from .base import BaseDistribution


class BetaDistribution(BaseDistribution):

    def __init__(
        self, 
        size: int,
        alpha_value: int=5,
        beta_value: int=6,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=beta(
                alpha_value,
                beta_value,
                loc=center,
                scale=randomness
            )
        )