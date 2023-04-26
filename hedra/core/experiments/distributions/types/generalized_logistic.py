from typing import Union
from scipy.stats import genlogistic
from .base import BaseDistribution


class GeneralizedLogisticDistribution(BaseDistribution):

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
            frozen_distribution=genlogistic(
                a_value,
                loc=center,
                scale=randomness
            )
        )
