from scipy.stats import cosine
from .base import BaseDistribution


class CrystalDistribution(BaseDistribution):

    def __init__(
        self,
        size: int
    ):
        center = int(size * 0.5)
        scale_factor = int(size * 0.1)
        super().__init__(
            size,
            center,
            center,
            cosine(
                1,
                size,
                loc=center,
                scale=scale_factor
            )
        )

    def generate(self):
        return super().generate()/self.size
