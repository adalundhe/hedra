from scipy.stats import arcsine
from .base import BaseDistribution


class ArcsineDistribution(BaseDistribution):

    def __init__(self, size: int):
        center = int(size/2)
        scale_factor = size * 0.4

        super().__init__(
            size,
            center,
            scale_factor,
            arcsine(
                loc=center,
                scale=scale_factor
            )
        )