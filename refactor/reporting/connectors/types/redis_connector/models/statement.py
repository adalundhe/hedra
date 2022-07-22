import json


class Statement:

    def __init__(self, query):
        self.key = query.get('key')
        self.value = query.get('value')
        self.type = query.get('type')
        self.channel = query.get('channel')
        self.options = query.get('options', {})

    def to_record(self):
        key = self.key
        value = self.value

        if self.channel:
            key = self.channel

        if self.options.get('serialize'):
            value = json.dumps(self.value)

        return [key, value]

    def to_query(self):
        return self.key

    def to_stream_record(self):
        return [self.key, dict(self.value)]

    def to_stream_query(self):

        if self.value is None:
            self.value = '$'

        return [
            {self.key: self.value},
            self.options.get('records_count'),
            self.options.get('timeout', 0)
        ]

    def to_key_query(self):

        key = self.key
        if key is None:
            key = '*'

        return u'{key}'.format(key=key)

            