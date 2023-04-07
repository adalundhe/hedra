from scipy.stats import cauchy
from .base import BaseDistribution


class CauchyDistribution(BaseDistribution):

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
            cauchy(
                loc=center,
                scale=scale
            )
        )