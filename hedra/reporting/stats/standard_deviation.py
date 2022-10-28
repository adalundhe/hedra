import math
from .variance import (
    Variance
)

class StandardDeviation:

    __slots__ = (
        'variance'
    )

    def __init__(self):
        self.variance = Variance()

    def update(self, new_value):
        self.variance.update(new_value)
        return self

    def get(self):
        return math.sqrt(self.variance.get())
