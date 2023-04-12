from typing import Union
from scipy.stats import f
from .base import BaseDistribution


class FDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        distribution_function_number: Union[int, float]=10,
        distribution_function_density: Union[int, float]=6,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=f(
                distribution_function_number,
                distribution_function_density,
                loc=center,
                scale=randomness
            )
        )
