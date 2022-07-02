import struct
from codecs import StreamReader
from typing import Tuple
from hedra.core.engines.types.common.constants import HEADER_LENGTH_INDEX


async def websocket_headers_to_iterator(reader: StreamReader):
    
    while True:
        # StreamReader already buffers data reading so it is efficient.
        res_data = await reader.readline()
        if b": " not in res_data and b":" not in res_data:
            break
  
        decoded = res_data.rstrip().decode()
        pair = decoded.split(": ", 1)
        if pair and len(pair) < 2:
            pair = decoded.split(":")

        key, value = pair
        yield key.lower(), value, res_data


def get_header_bits(raw_headers: bytes):

    b1 = raw_headers[0]
    fin = b1 >> 7 & 1
    rsv1 = b1 >> 6 & 1
    rsv2 = b1 >> 5 & 1
    rsv3 = b1 >> 4 & 1
    opcode = b1 & 0xf
    b2 = raw_headers[1]
    has_mask = b2 >> 7 & 1
    length_bits = b2 & 0x7f

    header_bits = (fin, rsv1, rsv2, rsv3, opcode, has_mask, length_bits)

    return header_bits


async def get_message_buffer_size(header_bits: Tuple[int], reader: StreamReader):
        bits = header_bits[HEADER_LENGTH_INDEX]
        length_bits = bits & 0x7f
        length = 0
        if length_bits == 0x7e:
            v = await reader.read(2)
            length = struct.unpack("!H", v)[0]
        elif length_bits == 0x7f:
            v = await reader.read(8)
            length = struct.unpack("!Q", v)[0]
        else:
            length = length_bits

        return length
