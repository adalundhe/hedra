import asyncio
import sys
import socket
import struct
import time
import functools
import uuid
from typing import Tuple, Any
from .ping_type import PingType

if sys.platform.startswith("win"):
    if sys.version_info[0] > 3 or (sys.version_info[0] == 3 and sys.version_info[1] >= 8):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# ICMP types, see rfc792 for v4, rfc4443 for v6
ICMP_ECHO_REQUEST = 8
ICMP6_ECHO_REQUEST = 128
ICMP_ECHO_REPLY = 0
ICMP6_ECHO_REPLY = 129


class PingConnection:

    def __init__(self) -> None:
        self.proto_icmp = socket.getprotobyname("icmp")
        self.proto_icmp6 = socket.getprotobyname("ipv6-icmp")


    def checksum(self, buffer: bytes) -> bytes:
        """
        I'm not too confident that this is right but testing seems
        to suggest that it gives the same answers as in_cksum in ping.c
        :param buffer:
        :return:
        """
        sum = 0
        count_to = (len(buffer) / 2) * 2
        count = 0

        while count < count_to:
            this_val = buffer[count + 1] * 256 + buffer[count]
            sum += this_val
            sum &= 0xffffffff # Necessary?
            count += 2

        if count_to < len(buffer):
            sum += buffer[len(buffer) - 1]
            sum &= 0xffffffff # Necessary?

        sum = (sum >> 16) + (sum & 0xffff)
        sum += sum >> 16
        answer = ~sum
        answer &= 0xffff

        # Swap bytes. Bugger me if I know why.
        answer = answer >> 8 | (answer << 8 & 0xff00)

        return answer

    async def receive(
        self,
        active_socket: socket.socket, 
        id_: bytes, 
        timeout: float
    ) -> None:
        """
        receive the ping from the socket.
        :param my_socket:
        :param id_:
        :param timeout:
        :return:
        """
        loop = asyncio.get_event_loop()

        try:
            start = time.monotonic()
            elapsed = 0

            while elapsed < timeout:
                rec_packet = await loop.sock_recv(active_socket, 1024)
                time_received = time.perf_counter()

                if active_socket.family == socket.AddressFamily.AF_INET:
                    offset = 20
                else:
                    offset = 0

                icmp_header = rec_packet[offset:offset + 8]

                type, code, checksum, packet_id, sequence = struct.unpack(
                    "bbHHh", icmp_header
                )

                if type != ICMP_ECHO_REPLY and type != ICMP6_ECHO_REPLY:
                    continue

                if packet_id == id_:
                    data = rec_packet[offset + 8:offset + 8 + struct.calcsize("d")]
                    time_sent = struct.unpack("d", data)[0]

                    return time_received - time_sent
                
                elapsed = time.monotonic() - start

        except asyncio.TimeoutError:
            asyncio.get_event_loop().remove_writer(active_socket)
            asyncio.get_event_loop().remove_reader(active_socket)
            active_socket.close()

            raise TimeoutError("Ping timeout")


    def sendto_ready(
        self, 
        packet: bytes, 
        socket: socket.socket, 
        future: asyncio.Future, 
        dest: Tuple[str, ...]
    ):
        try:
            socket.sendto(packet, dest)
        except (BlockingIOError, InterruptedError):
            return  # The callback will be retried
        except Exception as exc:
            asyncio.get_event_loop().remove_writer(socket)
            future.set_exception(exc)
        else:
            asyncio.get_event_loop().remove_writer(socket)
            future.set_result(None)


    async def send(
        self,
        active_socket: socket.socket, 
        dest_addr: Tuple[str, ...], 
        id_: bytes, 
        family: int
    ):
        """
        Send one ping to the given >dest_addr<.
        :param my_socket:
        :param dest_addr:
        :param id_:
        :param timeout:
        :return:
        """
        icmp_type = ICMP_ECHO_REQUEST if family == socket.AddressFamily.AF_INET\
            else ICMP6_ECHO_REQUEST

        # Header is type (8), code (8), checksum (16), id (16), sequence (16)
        ping_checksum = 0

        # Make a dummy header with a 0 checksum.
        header = struct.pack(
            "BbHHh", 
            icmp_type, 
            0, 
            ping_checksum, 
            id_, 
            1
        )

        bytes_in_double = struct.calcsize("d")
        data = (192 - bytes_in_double) * "Q"
        data = struct.pack(
            "d", 
            time.perf_counter()
        ) + data.encode("ascii")

        # Calculate the checksum on the data and the dummy header.
        ping_checksum = self.checksum(header + data)

        # Now that we have the right checksum, we put that in. It's just easier
        # to make up a new header than to stuff it into the dummy.
        header = struct.pack(
            "BbHHh", 
            icmp_type, 
            0, 
            socket.htons(ping_checksum), 
            id_, 
            1
        )
        packet = header + data

        future = asyncio.get_event_loop().create_future()

        callback = functools.partial(
            self.sendto_ready, 
            packet=packet, 
            socket=active_socket, 
            dest=dest_addr, 
            future=future
        )

        asyncio.get_event_loop().add_writer(
            active_socket, 
            callback
        )

        await future


    async def ping(
        self,
        socket_config: Tuple[Any, ...]=None, 
        ping_type: PingType=PingType.ICMP,
        timeout: int=10
    ):
        """
        Returns either the delay (in seconds) or raises an exception.
        :param dest_addr:
        :param timeout:
        :param family:
        """

        family, type_, proto, _, address = socket_config

        if ping_type == PingType.ICMP:

            if family == socket.AddressFamily.AF_INET:
                proto = self.proto_icmp
            else:
                proto = self.proto_icmp6

        try:
            my_socket = socket.socket(family=family, type=type_, proto=proto)
            my_socket.setblocking(False)

        except OSError as e:
            msg = e.strerror

            if e.errno == 1:
                # Operation not permitted
                msg += (
                    " - Note that ICMP messages can only be sent from processes"
                    " running as root."
                )

                raise OSError(msg)

            raise

        my_id = uuid.uuid4().int & 0xFFFF

        if ping_type == PingType.ICMP:
            await self.send(my_socket, address, my_id, family)
            response = await self.receive(my_socket, my_id, timeout)

        my_socket.close()

        return response

