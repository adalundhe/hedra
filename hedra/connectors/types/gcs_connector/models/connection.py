import os
from google.cloud import storage
from google.cloud.storage import Blob
from async_tools.functions import awaitable
from .types import GCSResultsCollection


class Connection:

    def __init__(self, config) -> None:
        self.config = config

        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.config.get(
            'google_application_credentials_path',
            os.getenv('GOOGLE_APPLICATION_CREDENTIALS_PATH', '/google/keyfile.json')
        )

        self.client = storage.Client()
        self.results = GCSResultsCollection()

    async def connect(self):
        pass

    async def execute(self, statement):

        bucket = await awaitable(self.client.get_bucket, statement.bucket)

        if statement.type == 'put':
            await statement.prepare()

            blob = Blob(statement.key, bucket)
            await awaitable(
                blob.upload_from_string,
                statement.gcs_item.data
            )

        elif statement.type == 'list':
            results = await awaitable(bucket.list_blobs)
            await self.results.update_bucket_objects(results)

        elif statement.type == 'delete':
            blob = Blob(statement.key, bucket)
            await awaitable(blob.delete)

        else:
            blob = Blob(statement.key, bucket)
            result = await awaitable(self.client.download_as_bytes, blob)

            await self.results.store(
                result,
                statement.bucket,
                statement.key
            )

    async def commit(self):
        return self.results.items()

    async def clear(self):
        self.results = GCSResultsCollection()

    async def close(self):
        await self.clear()
