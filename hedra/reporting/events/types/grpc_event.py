import binascii
from hedra.core.engines.types.common.response import Response
from .http2_event import HTTP2Event


class GRPCEvent(HTTP2Event):

    def __init__(self, response: Response) -> None:
        super().__init__(response)

    def grpc_decode(self, protobuf):
        wire_msg = binascii.b2a_hex(self.data)

        message_length = wire_msg[4:10]
        msg = wire_msg[10:10+int(message_length, 16)*2]
        protobuf.ParseFromString(binascii.a2b_hex(msg))

        return protobuf