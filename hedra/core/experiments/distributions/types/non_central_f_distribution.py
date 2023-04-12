import random
from typing import Union
from scipy.stats import ncf
from .base import BaseDistribution


class NonCenteralFDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        distribution_function_number: Union[int, float]=0.5,
        distribution_function_density: Union[int, float]=random.uniform(0, 10),
        nc_value: Union[int,float]=0.1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=ncf(
                distribution_function_number,
                distribution_function_density,
                nc_value,
                loc=center,
                scale=randomness
            )
        )
