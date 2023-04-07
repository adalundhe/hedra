from scipy.stats import beta
from .base import BaseDistribution


class BetaDistribution(BaseDistribution):

    def __init__(
        self, 
        size: int,
        alpha_value: int=2,
        beta_value: int=6
    ):
        center = int(size/2)

        super().__init__(
            size,
            center,
            None,
            beta(
                alpha_value,
                beta_value
            )
        )