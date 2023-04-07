from scipy.stats import anglit
from .base import BaseDistribution


class AnglitDistribution(BaseDistribution):

    def __init__(self, size: int):
        center = int(size/2)
        scale_factor = size * 0.4

        super().__init__(
            size,
            center,
            scale_factor,
            anglit(
                loc=center,
                scale=scale_factor
            )
        )