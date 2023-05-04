from typing import List, Dict
from .stream import Stream


class StreamAnalytics:

    def __init__(self) -> None:    
        self.interval_completion_rates: List[float] = []
        self.interval_timings: List[Dict[str, float]] = []
        self.interval_completed_counts: List[int] = []
        self.interval_succeeded_counts: List[int] = []
        self.interval_failed_counts: List[int] = []
        self.interval_batch_timings: List[float] = []

    def add(
            self, 
            stream: Stream,
            batch_elapsed: float
        ):

        completed_count = len(stream.completed)
        self.interval_completion_rates.append(
            completed_count/batch_elapsed
        )

        self.interval_completed_counts.append(completed_count)
        self.interval_succeeded_counts.append(stream.succeeded)
        self.interval_failed_counts.append(stream.failed)
        self.interval_timings.append(stream.timings)
        self.interval_batch_timings.append(batch_elapsed)

    

