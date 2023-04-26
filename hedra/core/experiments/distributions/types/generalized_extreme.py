from typing import Union
from scipy.stats import genextreme
from .base import BaseDistribution


class GeneralizedExtremeDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        c_value: float=0.5,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=genextreme(
                c_value,
                loc=center,
                scale=randomness
            )
        )
