from typing import Union
from scipy.stats import ncx2
from .base import BaseDistribution


class NonCenteralChiSquaredDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        distribution_function: Union[int, float]=1,
        nc_value: Union[int,float]=0.5,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=ncx2(
                distribution_function,
                nc_value,
                loc=center,
                scale=randomness
            )
        )
