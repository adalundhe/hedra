from typing import Union
from scipy.stats import kstwobign
from .base import BaseDistribution


class KSTwoBinomialGeneralizedDistribution(BaseDistribution):

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
            frozen_distribution=kstwobign(
                loc=center,
                scale=randomness
            )
        )
