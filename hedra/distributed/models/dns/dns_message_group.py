from typing import List
from .dns_message import DNSMessage
from .message import Message


class DNSMessageGroup(Message):
    messages: List[DNSMessage]