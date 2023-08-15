import io
import struct
from typing import Dict
from .pack_string import pack_string

def pack_domain_name(
    name: bytes, 
    names: Dict[bytes, bytes], 
    offset: int=0
):

    parts = name.split('.')
    buf = io.BytesIO()

    while parts:

        subname = '.'.join(parts)
        u = names.get(subname)

        if u:
            buf.write(struct.pack('!H', 0xc000 + u))
            break

        else:
            names[subname] = buf.tell() + offset

        buf.write(
            pack_string(
                parts.pop(0)
            )
        )

    else:
        buf.write(b'\0')
        
    return buf.getvalue()
