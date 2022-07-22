import json

from async_tools.functions.awaitable import awaitable

class GCSItem:

    def __init__(self, item={}) -> None:
        self.bucket = item.get('bucket')
        self.key = item.get('key')
        self.data = item.get('data')
        self.data_type = item.get('data_type')
        self.encoding = item.get('encoding', 'utf-8')

    async def from_gcs(self, gcs_object, bucket, key):
        self.bucket = bucket
        self.key = key
        self.data = gcs_object

    async def to_object(self):

        if self.data_type == 'json':
            self.data = json.dumps(self.data)

        elif self.data_type == 'file':
            self.data = open(self.data)

        else:
            self.data = bytes(self.data, self.encoding)