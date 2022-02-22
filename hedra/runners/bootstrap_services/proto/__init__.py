from .bootstrap_pb2 import (
    ServiceRegistrationStatusRequest,
    ServiceRegistrationStatusResponse,
    ServiceRegisterRequest
)


from .bootstrap_pb2_grpc import (
    BootstrapServerServicer,
    BootstrapServerStub,
    add_BootstrapServerServicer_to_server
)