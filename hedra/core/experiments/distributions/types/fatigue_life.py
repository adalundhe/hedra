from scipy.stats import fatiguelife
from .base import BaseDistribution


class FDistribution(BaseDistribution):

    def __init__(
        self,
        size: int
    ):
        center = int(size * 0.1)
        scale_factor = int(size * 0.25)
        super().__init__(
            size,
            center,
            center,
            fatiguelife(
                0.5,
                loc=center,
                scale_factor=scale_factor
            )
        )

    def generate(self):
        return super().generate()/self.size
