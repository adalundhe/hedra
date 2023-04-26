from typing import Union
from scipy.stats import burr
from .base import BaseDistribution


class BurrDistribution(BaseDistribution):

    def __init__(
        self, 
        size: int, 
        c_value=2,
        d_value=1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ) -> None:
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=burr(
                c_value,
                d_value,
                center=center,
                randomness=randomness,
            )
        )