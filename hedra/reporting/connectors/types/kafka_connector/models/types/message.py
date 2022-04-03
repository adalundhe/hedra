class Message:

    def __init__(self, message={}):
        self.type = message.get('type')
        self.topic = message.get('message_topic')
        self.value = message.get('message_value')
        self.partition = message.get('message_partition', 0)
        self.headers = message.get('message_headers')
        self.timestamp = message.get('message_timestamp')
        self.options = message.get('options', {})

        self.key = message.get('message_key', None)
        if self.key:
            self.key = bytes(self.key, 'utf')


    def from_kafka_message(self, kafka_message) -> None:
        self.topic = kafka_message.topic
        self.value = kafka_message.value
        self.partition = kafka_message.partition
        self.key = kafka_message.key
        self.headers = list(kafka_message.headers)
        self.timestamp = kafka_message.timestamp

    def to_dict(self) -> dict:
        return {
            'topic': self.topic,
            'value': self.value,
            'partition': self.partition,
            'key': str(self.key),
            'headers': self.headers,
            'timestamp': self.timestamp
        }