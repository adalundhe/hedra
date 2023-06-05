from typing import Dict, Any


class ProtobufRegistry:

    def __init__(self) -> None:
        self._protobufs: Dict[str, Any]

    def __getitem__(self, action_name : str) -> Any:
        return self._protobufs.get(action_name)

    def __setitem__(self, action_name: str, protobuf: Any):
        self._protobufs[action_name] = protobuf

    def get(self, action_name) -> Any:
        return self._protobufs.get(Any)


def make_protobuf_registry():
    return ProtobufRegistry()


protobuf_registry = make_protobuf_registry()