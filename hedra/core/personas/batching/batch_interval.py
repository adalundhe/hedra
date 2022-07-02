import random


class BatchInterval:
    
    def __init__(self, config) -> None:
        self.wait_period = config.get('batch_interval', 1)
        interval_range = config.get('batch_interval_range')
        self.period_min = None
        self.period_max = None
        
        if interval_range:
            interval_range = interval_range.split(':')
            self.period_min = int(interval_range[0])
            self.period_max = int(interval_range[1])
            self.interval_type = 'range'

        else:
            self.interval_type = 'static'

    @classmethod
    def about(cls):
        return '''
        Batch Interval

        key arguments:

        --batch-interval <fixed_interval> (defaults to 1 second)

        --batch-interval-range <min_interval>:<max_interval> (defaults to 1:5 seconds)

        Batch intervals allow for either fixed or randomized wait times between execution of batches. Fixed intervals
        are specified via the --batch-interval argument as an (integer) number of seconds. Randomized intervals
        are specified as a colon-seperated pair of (integer) seconds via the --batch-interval-range argument. For
        further information on how each persona implements batch intervals, view the documentation for that persona via
        the command:

            hedra --about personas:<persona_type>


        Note that excessive waits between batches will greatly reduce Hedra's throughput.


        Related Topics:

        - batches
        - personas

        '''

    def __mul__(self, other):
        if self.interval_type == 'range':
            return self.period_max * other

        else:
            return self.wait_period * other

    def __rmul__(self, other):
        return self.__mul__(other)

    @property
    def period(self):
        if self.interval_type == 'range':
            return random.randint(self.period_min, self.period_max)

        else:
            return self.wait_period