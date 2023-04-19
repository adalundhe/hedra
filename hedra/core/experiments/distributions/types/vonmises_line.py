from typing import Union
from scipy.stats import vonmises_line
from .base import BaseDistribution


class VonMisesLineDistribution(BaseDistribution):

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
            frozen_distribution=vonmises_line(
                alpha,
                loc=center,
                scale=randomness
            )
        )
