class StreamConfig:

    def __init__(self, stream):
        self.name = stream.get('stream_name')
        self.fields = stream.get('fields', {})

    def to_dict(self):
        return {
            'stream_name': self.name,
            'fields': self.fields
        }