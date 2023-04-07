from scipy.stats import bradford
from .base import BaseDistribution


class BradfordDistribution(BaseDistribution):

    def __init__(
        self, 
        size: int
    ):
        center = int(size/2)
        scale_factor = None

        super().__init__(
            size,
            center,
            scale_factor,
            bradford(0.5)
        )