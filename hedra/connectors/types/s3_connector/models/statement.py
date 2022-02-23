from .types import S3Item


class Statement:

    def __init__(self, statement) -> None:
        self.s3_item = S3Item(statement)
        self.key = statement.get('key')
        self.bucket = statement.get('bucket')
        self.type = statement.get('type')

    async def prepare(self):
        await self.s3_item.to_object()