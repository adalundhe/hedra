from typing import Union
from scipy.stats import exponpow
from .base import BaseDistribution


class ExponentialPowerDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        center: float = 0.1,
        scale: Union[int, float]=2
    ):
        super().__init__(
            size,
            center,
            scale,
            exponpow(
                scale,
                center
            )
        )
