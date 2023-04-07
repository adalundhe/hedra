from scipy.stats import chi
from .base import BaseDistribution


class ChiSquaredDistribution(BaseDistribution):

    def __init__(
        self,
        size: int
    ):
        center = int(size * 0.1)
        scale_factor = int(size * 0.5)

        super().__init__(
            size,
            center,
            scale_factor,
            chi(
                loc=center,
                scale=scale_factor
            )
        )

    def generate(self):
        return super().generate()/self.size