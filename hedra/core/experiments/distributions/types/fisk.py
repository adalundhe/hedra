from scipy.stats import fisk
from .base import BaseDistribution


class FiskDistribution(BaseDistribution):

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
            fisk(
                center,
                loc=center,
                scale_factor=scale_factor
            )
        )

    def generate(self):
        return super().generate()/self.size
