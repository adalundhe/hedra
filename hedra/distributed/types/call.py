from hedra.distributed.models.message import Message
from typing import TypeVar, Tuple


T = TypeVar('T', bound=(Message,))


Call = Tuple[int, T]