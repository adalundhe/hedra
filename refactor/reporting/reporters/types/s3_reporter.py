from __future__ import annotations
import os
from hedra.reporting.connectors.types.s3_connector import S3Connector



class S3Reporter:

    def __init__(self, config):
        self.format = 's3'
        self.reporter_config = config
        self.session = None
        
        buckets_config = self.reporter_config.get('bucket_config')
        self.metrics_bucket = buckets_config.get('metrics_bucket', os.getenv('S3_METRICS_BUCKET', 'metrics'))

        self.connector = S3Connector(self.reporter_config)

    @classmethod
    def about(cls):
        return '''
        S3 Reporter - (s3)

        The S3 reporter allows you to store metrics and events in S3 "buckets". Like the Google Cloud Storage reporter, 
        the S3 reporter serializes events and metrics as JSON objects, deserializing the former when fetching their aggregate 
        results. To specify what buckets the reporter should use, pass the names of the buckets as below:

        {
            "reporter_config": {
                "bucket_config": {
                    "events_bucket": "<EVENTS_BUCKET_NAME_HERE>",
                    "metrics_bucket": "<METRICS_BUCKET_NAME_HERE>"
                }
            }
        }
        
        Alternatively you may set the S3_EVENTS_BUCKET and/or S3_METRICS_BUCKET environmental variables.

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

    async def init(self) -> S3Reporter:
        await self.connector.connect()
        return self

    async def submit(self, metric) -> S3Reporter:
        await self.connector.execute({
            'bucket': self.metrics_bucket,
            'key': metric.metric.metric_name,
            'data': metric.to_dict(),
            'data_type': 'json',
            'type': 'put'
        })
        
        return self

    async def close(self) -> S3Reporter:
        await self.connector.clear()
        await self.connector.close()
        
        return self


