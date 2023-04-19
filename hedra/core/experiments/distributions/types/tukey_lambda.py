from typing import Union
from scipy.stats import tukeylambda
from .base import BaseDistribution


class TukeyLambdaDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        a_value: float=1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.5
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=tukeylambda(
                a_value,
                loc=center,
                scale=randomness
            )
        )
