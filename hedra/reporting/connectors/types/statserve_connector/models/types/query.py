class Query:

    def __init__(self, query):
        self.stream_name = query.get('stream_name')
        self.key = query.get('key')
        self.stat_type = query.get('stat_type')
        self.stat_name = query.get('stat_name')

    def to_dict(self):
        return {
            'stream_name': self.stream_name,
            'key': self.key,
            'type': self.stat_type,
            'stat': self.stat_name
        }