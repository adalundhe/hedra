from typing import Union
from scipy.stats import wrapcauchy
from .base import BaseDistribution


class WrappedCauchyDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        a_value: float=0.1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.1
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=wrapcauchy(
                a_value,
                loc=center,
                scale=randomness
            )
        )
