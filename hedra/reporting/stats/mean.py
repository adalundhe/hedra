class Mean:

    __slots__ = (
        'size',
        'previous_size',
        'mean',
        'delta',
        'delta_n'
    )

    def __init__(self):
        self.size = 0
        self.previous_size = 0
        self.mean = 0.0
        self.delta = 0.0
        self.delta_n = 0.0

    def update(self, new_value):
        self.previous_size = self.size
        self.size += 1
        self.delta = new_value - self.mean
        self.delta_n = self.delta / self.size
        self.mean += self.delta_n

        return self

    def get(self):
        return self.mean
