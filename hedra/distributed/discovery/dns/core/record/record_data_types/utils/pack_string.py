import struct
from typing import Union


def pack_string(string: Union[str, bytes], btype='B') -> bytes:
    '''Pack string into `{length}{data}` format.'''
    if not isinstance(string, bytes):
        string = string.encode()
    length = len(string)
    return struct.pack('%s%ds' % (btype, length), length, string)
