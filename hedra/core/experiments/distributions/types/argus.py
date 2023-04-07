from scipy.stats import argus
from .base import BaseDistribution


class ArgusDistribution(BaseDistribution):

    def __init__(self, size: int):
        center = int(size/2)
        scale_factor = size * 0.4

        super().__init__(
            size,
            center,
            scale_factor,
            argus(
                1,
                loc=center,
                scale=scale_factor
            )
        )