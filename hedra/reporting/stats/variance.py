from .mean import Mean

class Variance:

    __slots__ = (
        'mean',
        't1',
        't1_sum'
    )

    def __init__(self):
        self.mean = Mean()
        self.t1 = 0.0
        self.t1_sum = 0

    def update(self, new_value):
        self.mean.update(new_value)
        self.t1 = self.mean.delta * self.mean.delta_n * self.mean.previous_size
        self.t1_sum += self.t1
        
    def get(self):
        divisor = (self.mean.size - 1.0)
        if divisor == 0:
            divisor = 1

        return self.t1_sum/divisor