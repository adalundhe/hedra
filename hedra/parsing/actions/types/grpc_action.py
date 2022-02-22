import grpc
import os
import importlib
import sys


class GrpcAction:

    def __init__(self, action, group=None) -> None:
        self.name = action.get('name')
        self.host = action.get('host', group)
        self.proto_path = action.get('proto_path')
        self.proto_name = action.get('proto_name')
        self.rpc_name = action.get('endpoint')
        self.data = action.get('data')
        self.auth = action.get('auth')
        self.streaming_type = action.get('streaming_type')
        self.order = action.get('order')
        self.weight = action.get('weight')
        self.user = action.get('user')
        self.tags = action.get('tags', [])
        self.is_setup = action.get('is_setup', False)
        self.is_teardown = action.get('is_teardown', False)

        self._proto_file_stub = None
        self.protobuf = None
        self.stub = None
        self._pb2_module = None
        self._pb2_gprc_module = None
        self._proto_dir = None
        self._cwd = None
        self._rpc = None

        self.action_type = 'grpc'
        self._iterator = None
        self.execute = None

    @classmethod
    def about(cls):
        return '''
        GRPC Action

        GRPC Actions represent a single GRPC call or a single GRPC stream request. Note
        that each Action will compile and import both _pb2.py and _pb2_grpc.py file when
        parsing data. This will generate these files locally as well.

        Actions are specified as:

        - proto_path: <path_to_proto_file_to_use> (i.e. protos/test.proto)
        - proto_name: <name_of_rpc_type_to_use>
        - endpoint: <name_of_rpc_function_to_use>
        - data: <data_to_pass_to_rpc_type>
        - streaming_type: <type_of_streaming> (should be - 'response', 'request', or 'bidirectional')
        - host: <host_address_or_ip_of_target> (defaults to the action's group)
        - headers: <headers_to_pass_to_graphql_session>
        - auth: <auth_to_pass_to_grpc_session>
        - name: <action_name>
        - user: <user_associated_with_action>
        - tags: <list_of_tags_for_aggregating_actions>
        - weight: (optional) <action_weighting_for_weighted_persona>
        - order: (optional) <action_order_for_sequence_personas>

        '''

    async def parse_data(self):
        self._cwd = os.getcwd()
        self._proto_file_stub = os.path.splitext(self.proto_path)[0]
        proto_file = f'{self._proto_file_stub}_pb2.py'
        proto_server_file = f'{self._proto_file_stub}_pb2_grpc.py'

        self._proto_dir = os.path.dirname(os.path.abspath(self.proto_path))
        os.system(f'python3 -m grpc_tools.protoc -I {self._cwd} --python_out=. --grpc_python_out=. {self.proto_path}')

        self._pb2_module = self._load_from_spec("protos", proto_file)
        self._pb2_gprc_module = self._load_from_spec("stubs", proto_server_file)

        self.protobuf = self._pb2_module.__getattribute__(self.proto_name)

        if self.auth:
            channel = grpc.aio.secure_channel(
                self.host, 
                grpc.ssl_channel_credentials(**self.auth)
            )

        else:
            channel = grpc.aio.insecure_channel(self.host)

        server_module_files = dir(self._pb2_gprc_module)
        stub_name = None

        for module_file in server_module_files:
            if "Stub" in module_file:
                stub_name = module_file
    
        self.stub = self._pb2_gprc_module.__getattribute__(stub_name)(channel)
        self._rpc = self.stub.__getattribute__(self.rpc_name)

        if self.streaming_type == 'response':
            self._iterator = self._iter_streaming_response_rpc
            self.execute = self.execute_streaming_rpc

        elif self.streaming_type == "request":
            self._iterator = None
            self.execute = self.execute_request_streaming_rpc

        elif self.streaming_type == "bidirectional":
            self._iterator = self._iter_bidirectional_stream_rpc
            self.execute = self.execute_streaming_rpc
        else:
            self._iterator = None
            self.execute = self.execute_rpc

    def _load_from_spec(self, module, proto_file):
        spec = importlib.util.spec_from_file_location(module, f'{self._cwd}/{proto_file}')
        if self._proto_dir not in sys.path:
            sys.path.append(self._proto_dir)

        module = importlib.util.module_from_spec(spec)

        sys.modules["protos"] = module
        spec.loader.exec_module(module)

        return module

    async def execute_rpc(self):
        return await self._rpc(self.protobuf(**self.data))

    async def execute_streaming_rpc(self):
        return [response async for response in self._iterator()]

    
    async def execute_request_streaming_rpc(self):
        return await self._rpc([message async for message in self._payload_as_generator()])

    async def _iter_streaming_response_rpc(self):
        async for response in self._rpc(self.protobuf(**self.data)):
            yield response

    async def _iter_bidirectional_stream_rpc(self):
        async for response in self._rpc([message async for message in self._payload_as_generator()]):
            yield response

    async def _payload_as_generator(self):
        for item in self.data:
            yield self.protobuf(**item)

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'host': self.host,
            'streaming_type': self.streaming_type,
            'endpoint': self.rpc_name,
            'user': self.user,
            'tags': self.tags,
            'url': self._proto_file_stub,
            'method': self.rpc_name,
            'data': self.data,
            'order': self.order,
            'weight': self.weight,
            'action_type': self.action_type
        }

    