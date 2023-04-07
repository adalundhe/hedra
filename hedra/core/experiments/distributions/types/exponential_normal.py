from scipy.stats import exponnorm
from .base import BaseDistribution


class ExponentialNormalDistribution(BaseDistribution):

    def __init__(
        self,
        size: int
    ):
        center = int(size * 0.1)
        scale_factor = int(size * 0.4)
        super().__init__(
            size,
            center,
            center,
            exponnorm(
                center,
                scale_factor
            )
        )

    def generate(self):
        return super().generate()/self.size
