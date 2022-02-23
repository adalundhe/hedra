from .message import Message


class MessageSet:

    def __init__(self, messages) -> None:
        self.messages = [Message(message) for message in messages]

    def __iter__(self):
        for message in self.messages:
            yield message

    async def __aiter__(self):
        for message in self.messages:
            yield message