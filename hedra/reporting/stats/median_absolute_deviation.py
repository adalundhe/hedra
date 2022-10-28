import tdigest


class MedianAbsoluteDeviation:

    __slots__ = (
        'approx_median',
        'update_interval',
        'records_processed',
        'values',
        'deviations',
        'constant'
    )
    
    def __init__(self, update_interval=5, sensitivity=0.01, compression_factor=25):
        self.approx_median = None
        self.update_interval = update_interval
        self.records_processed = 0
        self.values = tdigest.TDigest()
        self.deviations = tdigest.TDigest(delta=sensitivity,K=compression_factor)
        self.constant = 1.4826

    def update(self, new_value):
        if self.approx_median is None:
            self.approx_median = new_value

        self.values.update(new_value)
        deviation = abs(self.approx_median - new_value)
        self.deviations.update(deviation)

        self.records_processed += 1
        
        if self.records_processed%self.update_interval == 0:
            self.approx_median = self.values.percentile(50)

    def get(self):
        if len(self.deviations) > 0:
            return self.deviations.percentile(50) * self.constant
        else:
            return 0
