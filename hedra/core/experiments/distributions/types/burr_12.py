from scipy.stats import beta
from .base import BaseDistribution


class Burr12Distribution(BaseDistribution):

    def __init__(
        self, 
        size: int,
        c_value: int=8,
        d_value: int=6
    ):
        center = int(size/2)

        super().__init__(
            size,
            center,
            None,
            beta(
                c_value,
                d_value
            )
        )