from typing import Union
from scipy.stats import vonmises
from .base import BaseDistribution


class VonMisesDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: float=0.1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=vonmises(
                alpha,
                loc=center,
                scale=randomness
            )
        )
