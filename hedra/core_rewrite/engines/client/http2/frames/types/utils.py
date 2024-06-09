import binascii
import collections
from typing import Optional


def raw_data_repr(data: Optional[bytes]) -> str:
    if not data:
        return "None"
    r = binascii.hexlify(data).decode('ascii')
    if len(r) > 20:
        r = r[:20] + "..."
    return "<hex:" + r + ">"


HeaderValidationFlags = collections.namedtuple(
    'HeaderValidationFlags',
    ['is_client', 'is_trailer', 'is_response_header', 'is_push_promise']
)
