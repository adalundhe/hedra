import csv
from typing import Any, List
has_connector = True


class CSV:

    def __init__(self, config: Any) -> None:
        self.events_filepath = config.events_filepath
        self.metrics_filepath = config.metrics_filepath

    async def connect(self):
        pass

    async def submit_events(self, events: List[Any]):

        headers = events[0].fields
        csv_writer = csv.DictWriter(open(self.events_filepath, 'w'), fieldnames=headers)
        csv_writer.writeheader()

        for event in events:
            csv_writer.writerow(event.record)

    async def submit_metrics(self, metrics: List[Any]):

        headers = metrics[0].fields
        csv_writer = csv.DictWriter(open(self.metrics_filepath, 'w'), fieldnames=headers)
        csv_writer.writeheader()

        for metric in metrics:
            csv_writer.writerow(metric.record)

    async def close(self):
        pass

