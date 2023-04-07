from scipy.stats import f
from .base import BaseDistribution


class FDistribution(BaseDistribution):

    def __init__(
        self,
        size: int
    ):
        center = int(size * 0.1)
        scale_factor = int(size * 0.1)
        super().__init__(
            size,
            center,
            center,
            f(
                1,
                size,
                loc=center,
                scale_factor=scale_factor
            )
        )

    def generate(self):
        return super().generate()/self.size
