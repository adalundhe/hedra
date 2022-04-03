from __future__ import annotations
import os
from hedra.reporting.connectors.types.gcs_connector import GCSConnector


class GoogleCloudStorageReporter:

    def __init__(self, config):
        self.format = 'gcs'

        self.reporter_config = config

        buckets_config = self.reporter_config.get('bucket_config')
        self.events_bucket = buckets_config.get('events_bucket', os.getenv('GOOGLE_EVENTS_BUCKET', 'events'))
        self.metrics_bucket = buckets_config.get('metrics_bucket', os.getenv('GOOGLE_METRICS_BUCKET', 'metrics'))

        self.connector = GCSConnector(self.reporter_config)
        

    @classmethod
    def about(cls):
        return '''
        Google Cloud Storage Reporter - (gcs)

        The Google Cloud Storage reporter allows you to store metrics and events in Google Cloud Storage
        "buckets". Like the S3 reporter, the GCS reporter serializes events and metrics as JSON objects,
        deserializing the former when fetching their aggregate results. To specify what buckets the reporter
        should use, pass the names of the buckets as below:

        {
            "reporter_config": {
                "bucket_config": {
                    "events_bucket": "<EVENTS_BUCKET_NAME_HERE>",
                    "metrics_bucket": "<METRICS_BUCKET_NAME_HERE>"
                }
            }
        }

        Alternatively you may set the GOOGLE_EVENTS_BUCKET and/or GOOGLE_METRICS_BUCKET environmental variables.
        
        Note that if no events or metrics bucket exists, you may create them by passing the appropriate config 
        for the events/metrics bucket under the "bucket_config" key of the reporter config. For example:

        {
            "reporter_config": {
                "bucket_config": {
                    "events_bucket": {
                        ...(config here)
                    },
                    "metrics_bucket": {
                        ...(config here)
                    }
                }
            }
        }

        '''

    async def init(self) -> GoogleCloudStorageReporter:
        await self.connector.connect()
        return self

    async def update(self, event) -> list:

        await self.connector.execute({
            'bucket': self.events_bucket,
            'key': event.event.name,
            'data': event.to_dict(),
            'data_type': 'json',
            'type': 'put'
        })

        return [
            {
                'field': event.event.name,
                'message': 'OK'
            }
        ]

    async def merge(self, connector) -> GoogleCloudStorageReporter:
        return self

    async def fetch(self, key=None, stat_type=None, stat_field=None, partial=False) -> list:
        await self.connector.execute({
            'bucket': self.metrics_bucket,
            'key': key
        })

        return await self.connector.commit()

    async def submit(self, metric) -> GoogleCloudStorageReporter:
        await self.connector.execute({
            'bucket': self.metrics_bucket,
            'key': metric.metric.metric_name,
            'data': metric.to_dict(),
            'data_type': 'json',
            'type': 'put'
        })
        
        return self

    async def close(self) -> GoogleCloudStorageReporter:
        await self.connector.clear()
        await self.connector.close()
        
        return self



