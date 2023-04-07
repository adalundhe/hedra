from scipy.stats import burr
from .base import BaseDistribution


class BurrDistribution(BaseDistribution):

    def __init__(
        self, 
        size: int, 
        c_value=3,
        d_value=2,
    ) -> None:
        
        center = int(size/2)
        scale_factor = int(size * 0.1)
        
        super().__init__(
            size,
            center,
            scale_factor,
            burr(
                c_value,
                d_value,
                loc=center,
                scale=scale_factor
            )
        )

    def generate(self):
        return super().generate()/self.size