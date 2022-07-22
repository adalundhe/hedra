from .message import Message


class ResultsSet:

    def __init__(self):
        self.messages = {}

    async def add_message(self, kafka_messages):

        for partition in kafka_messages:
            messages = kafka_messages[partition]
            for message in messages:
                new_message = Message()
                new_message.from_kafka_message(message)

                if self.messages.get(new_message.topic):
                    self.messages[new_message.topic].append(new_message)

                else:
                    self.messages[new_message.topic] = [new_message]

    async def to_results(self):

        for topic in self.messages:
            self.messages[topic] = [
                message.to_dict() for message in self.messages[topic]
            ]

        return self.messages

    async def clear(self):
        self.messages = {}
