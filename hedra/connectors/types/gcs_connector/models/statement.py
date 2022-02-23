from .types import GCSItem


class Statement:

    def __init__(self, statement) -> None:
        self.gcs_item = GCSItem(statement)
        self.key = statement.get('key')
        self.bucket = statement.get('bucket')
        self.type = statement.get('type')

    async def prepare(self):
        await self.gcs_item.to_object()