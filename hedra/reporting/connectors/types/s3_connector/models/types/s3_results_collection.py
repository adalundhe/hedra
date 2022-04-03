from .s3_item import S3Item


class S3ResultsCollection:

    def __init__(self) -> None:
        self.s3_data = {}


    async def update_bucket_objects(self, bucket_response):

        bucket_name = bucket_response.get('Name')

        if self.s3_data.get(bucket_name) is None:
            self.s3_data[bucket_name] = {
                'metadata': bucket_response,
                'objects': {}
            }

        else:
            self.s3_data[bucket_name]['metadata'] = bucket_response


    async def store(self, s3_object, bucket_name, key):

        if self.s3_data.get(bucket_name) is None:
            self.s3_data[bucket_name] = {
                'metadata': {},
                'objects': {}
            } 

        s3_item = S3Item()
        await s3_item.from_s3(s3_object)
        self.s3_data[bucket_name]['objects'][key] = s3_item

    async def items(self):
        return [
            s3_item.data for s3_item in self.s3_data
        ]
