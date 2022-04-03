from .gcs_item import GCSItem


class GCSResultsCollection:

    def __init__(self) -> None:
        self.gcs_data = {}


    async def update_bucket_objects(self, bucket_response):

        bucket_name = bucket_response.get('Name')

        if self.gcs_data.get(bucket_name) is None:
            self.gcs_data[bucket_name] = {
                'metadata': bucket_response,
                'objects': {}
            }

        else:
            self.gcs_data[bucket_name]['metadata'] = bucket_response


    async def store(self, gcs_object, bucket_name, key):

        if self.gcs_data.get(bucket_name) is None:
            self.gcs_data[bucket_name] = {
                'metadata': {},
                'objects': {}
            } 

        gcs_data = GCSItem()
        await gcs_data.from_gcs(gcs_object)
        self.gcs_data[bucket_name]['objects'][key] = gcs_object

    async def items(self):
        return [
            gcs_item.data for gcs_item in self.gcs_data
        ]
