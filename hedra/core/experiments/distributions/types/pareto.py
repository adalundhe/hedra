from typing import Union
from scipy.stats import pareto
from .base import BaseDistribution


class ParetoDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        b_value: Union[int, float]=1,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):
        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=pareto(
                b_value,
                loc=center,
                scale=randomness
            )
        )
