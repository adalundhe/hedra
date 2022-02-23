
import boto3
import os
from async_tools.functions import awaitable
from .types import S3ResultsCollection



class Connection:

    def __init__(self, config) -> None:
        self.session = boto3.Session(
            aws_access_key_id=config.get(
                'aws_public_key', 
                os.getenv('AWS_PUBLIC_KEY')
            ),
            aws_secret_access_key=config.get(
                'aws_secret_key', 
                os.getenv('AWS_SECRET_KEY')
            )
        )

        self.s3 = None
        self.results = S3ResultsCollection()


    async def connect(self):
        self.s3 = self.session.resource('s3')

    async def execute(self, statement):

        if statement.type == 'put':
            await statement.prepare()
            await awaitable(
                self.s3.put_object,
                Body=statement.s3_item.data,
                Bucket=statement.bucket,
                Key=statement.key
            )

        elif statement.type == 'list':
            results = await awaitable(
                self.s3.list_objects,
                Bucket=statement.bucket
            )

            await self.results.update_bucket_objects(results)

        elif statement.type == 'delete':
            await awaitable(
                self.s3.delete_object,
                Bucket=statement.bucket,
                Key=statement.key
            )

        else:
            result = await awaitable(
                self.s3.get_object,
                Bucket=statement.bucket,
                Key=statement.key
            )

            await self.results.store(
                result,
                statement.bucket,
                statement.key
            )

    async def commit(self):
        return await self.results.items()

    async def clear(self):
        self.results = S3ResultsCollection()

    async def close(self):
        await self.clear()
