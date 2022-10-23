import binascii
from hedra.core.engines.types.common.types import RequestTypes
from hedra.core.engines.types.http2.result import HTTP2Result
from .action import GRPCAction


class GRPCResult(HTTP2Result):

    def __init__(self, action: GRPCAction, error: Exception = None) -> None: 
        super(GRPCResult, self).__init__(action, error)
        self.type = RequestTypes.GRPC

    @property
    def data(self):
        wire_msg = binascii.b2a_hex(self.body)

        message_length = wire_msg[4:10]
        msg = wire_msg[10:10+int(message_length, 16)*2]

        return binascii.a2b_hex(msg)

    def to_protobuf(self, protobuf):
        protobuf.ParseFromString(self.data)
        return protobuf