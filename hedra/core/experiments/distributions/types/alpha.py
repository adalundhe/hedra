from scipy.stats import alpha
from .base import BaseDistribution


class AlphaDistribution(BaseDistribution):

    def __init__(
        self, 
        size: int, 
        center: float=0.5, 
        scale: float=0.1
    ) -> None:

        super().__init__(
            size,
            center,
            scale,
            alpha(
                1,
                loc=center,
                scale=scale
            )
        )