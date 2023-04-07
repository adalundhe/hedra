from scipy.stats import cosine
from .base import BaseDistribution


class CosineDistribution(BaseDistribution):

    def __init__(
        self,
        size: int,
        center: float=0.5,
        scale: float=0.1
    ):

        super().__init__(
            size,
            center,
            scale,
            cosine(
                loc=center,
                scale=scale
            )
        )
