from .message_set import MessageSet
from .message_set import MessageSet


class Transaction:

    def __init__(self, transaction) -> None:
        self.messages = MessageSet(transaction.get('messages', []))
        self.type = transaction.get('type')
        self.options = transaction.get('options')