from typing import Union
from scipy.stats import truncpareto
from .base import BaseDistribution


class TruncatedParetoDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        alpha: float=1.,
        beta: float=10.,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=1
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=truncpareto(
                alpha,
                beta,
                loc=center,
                scale=randomness
            )
        )
