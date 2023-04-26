from typing import Union
from scipy.stats import genpareto
from .base import BaseDistribution


class GeneralizedParetoDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        a_value: float=0.5,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=genpareto(
                a_value,
                loc=center,
                scale=randomness
            )
        )
