from typing import Union
from scipy.stats import cauchy
from .base import BaseDistribution


class CauchyDistribution(BaseDistribution):

    def __init__(
        self, 
        size: int, 
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ) -> None:

        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=cauchy(
                loc=center,
                scale=randomness
            )
        )