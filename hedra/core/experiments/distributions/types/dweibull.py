from scipy.stats import dweibull
from .base import BaseDistribution


class DWeibullDistribution(BaseDistribution):

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
            dweibull(
                1,
                loc=center,
                scale=scale_factor
            )
        )

    def generate(self):
        return super().generate()/self.size
