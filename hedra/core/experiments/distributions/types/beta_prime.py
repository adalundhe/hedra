from scipy.stats import betaprime
from .base import BaseDistribution


class BetaPrimeDistribution(BaseDistribution):

    def __init__(
        self, 
        size: int,
        alpha_value: int=3,
        beta_value: int=6,
        scale: float=0.5
    ):
        center = int(size/2)

        super().__init__(
            size,
            center,
            scale,
            betaprime(
                alpha_value,
                beta_value, 
                scale=scale
            )
        )