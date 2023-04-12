from typing import Union
from scipy.stats import burr12
from .base import BaseDistribution


class Burr12Distribution(BaseDistribution):

    def __init__(
        self, 
        size: int,
        c_value: int=2,
        d_value: int=1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.4
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozed_distribution=burr12(
                c_value,
                d_value,
                loc=center,
                scale=randomness,
            )
        )