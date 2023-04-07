from scipy.stats import chi2
from .base import BaseDistribution


class ChiSquaredDistribution(BaseDistribution):

    def __init__(
        self,
        size: int
    ):
        center = int(size/2)

        super().__init__(
            size,
            center,
            None,
            chi2(center)
        )

    def generate(self):
        return super().generate()/self.size