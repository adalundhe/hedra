class BaseMetric:

    def __init__(self, stat=None, value=None, metadata=None):
        self.stat = stat
        self.value = value
        self.metadata = metadata
