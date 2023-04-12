from typing import Union
from scipy.stats import cosine
from .base import BaseDistribution


class CosineDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        center: Union[int, float]=0.5,
        randomness: Union[int, float]=0.25
    ):

        super().__init__(
            size=size,
            center=center,
            randomness=randomness,
            frozen_distribution=cosine(
                loc=center,
                scale=randomness
            )
        )
