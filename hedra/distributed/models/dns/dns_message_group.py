from typing import List
from hedra.distributed.models.base.message import Message

from .dns_message import DNSMessage

class DNSMessageGroup(Message):
    messages: List[DNSMessage]