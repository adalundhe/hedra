import os
import struct
from typing import Tuple
from base64 import encodebytes as base64encode
from .connection import WebsocketConnection
from .constants import HEADER_LENGTH_INDEX

def create_sec_websocket_key():
    randomness = os.urandom(16)
    return base64encode(randomness).decode('utf-8').strip()

def pack_hostname(hostname):
    # IPv6 address
    if ':' in hostname:
        return '[' + hostname + ']'

    return hostname


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


async def get_message_buffer_size(header_bits: Tuple[int], connection: WebsocketConnection):
        bits = header_bits[HEADER_LENGTH_INDEX]
        length_bits = bits & 0x7f
        length = 0
        if length_bits == 0x7e:
            v = await connection.readexactly(2)
            length = struct.unpack("!H", v)[0]
        elif length_bits == 0x7f:
            v = await connection.readexactly(8)
            length = struct.unpack("!Q", v)[0]
        else:
            length = length_bits

        return length
