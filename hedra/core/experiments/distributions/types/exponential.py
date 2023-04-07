from scipy.stats import expon
from .base import BaseDistribution


class ExponentialDistribution(BaseDistribution):

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
            expon(
                center,
                scale_factor
            )
        )

    def generate(self):
        return super().generate()/self.size
