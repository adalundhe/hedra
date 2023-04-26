from typing import Union
from scipy.stats import pearson3
from .base import BaseDistribution


class Pearson3Distribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        kappa: Union[int, float]=0.5,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=pearson3(
                kappa,
                loc=center,
                scale=randomness
            )
        )
