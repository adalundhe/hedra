from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.http2.action import HTTP2Action
from hedra.core.engines.types.http2.client import MercuryHTTP2Client
from hedra.core.engines.types.http2.result import HTTP2Result
from hedra.core.engines.types.common.types import RequestTypes
from hedra.data.serializers.serializer_types.common.base_serializer import BaseSerializer
from typing import List, Dict, Union, Any


class HTTP2Serializer(BaseSerializer):

    def __init__(self) -> None:
        super().__init__()

    def action_to_serializable(
        self,
        action: HTTP2Action
    ) -> Dict[str, Union[str, List[str]]]:
        serialized_action = super().action_to_serializable(action)

        return {
            **serialized_action,
            'type': RequestTypes.HTTP2,
            'url': {
                'full': action.url.full,
                'ip_addr': action.url.ip_addr,
                'socket_config': action.url.socket_config,
                'has_ip_addr': action.url.has_ip_addr
            },
            'method': action.method,
            'headers': action._headers,
            'data': action.data,
            'is_stream': action.is_stream,
            'is_setup': action.is_setup,
            'action_args': action.action_args,
        }
    
    def deserialize_action(
        self,
        action: Dict[str, Any]
    ) -> HTTP2Action:
        
        url_config = action.get('url', {})
        metadata = action.get('metadata', {})

        http2_action = HTTP2Action(
            name=action.get('name'),
            url=url_config.get('full'),
            method=action.get('method'),
            headers=action.get('headers'),
            data=action.get('data'),
            user=metadata.get('user'),
            tags=metadata.get('tags', [])
        )

        http2_action.url.ip_addr = url_config.get('ip_addr')
        http2_action.url.socket_config = url_config.get('socket_config')
        http2_action.url.has_ip_addr = url_config.get('has_ip_addr')

        http2_action.setup()

        return http2_action
    
    def deserialize_client_config(self, client_config: Dict[str, Any]) -> MercuryHTTP2Client:
        return MercuryHTTP2Client(
            concurrency=client_config.get('concurrency'),
            timeouts=Timeouts(
                **client_config.get('timeouts', {})
            ),
            reset_connections=client_config.get('reset_sessions')
        )
    
    def result_to_serializable(
        self,
        result: HTTP2Result
    ) -> Dict[str, Any]:
        
        serialized_result = super().result_to_serializable(result)

        encoded_headers = {
            str(k.decode()): str(v.decode()) for k, v in result.headers.items()
        }

        data = result.data
        if isinstance(data, bytes) or isinstance(data, bytearray):
            data = str(data.decode())

        return {
            **serialized_result,
            'url': result.url,
            'method': result.method,
            'path': result.path,
            'params': result.params,
            'query': result.query,
            'type': result.type,
            'headers': encoded_headers,
            'data': data,
            'tags': result.tags,
            'user': result.user,
            'error': str(result.error),
            'status': result.status,
            'reason': result.reason,
        }
    
    def deserialize_result(
        self,
        result: Dict[str, Any]
    ) -> HTTP2Result:
        deserialized_result = HTTP2Result(
            HTTP2Action(
                name=result.get('name'),
                url=result.get('url'),
                method=result.get('method'),
                headers=result.get('headers'),
                data=result.get('data'),
                user=result.get('user'),
                tags=result.get('tags', [])      
            ),
            error=Exception(result.get('error'))
        )

        deserialized_result.status = result.get('status')
        deserialized_result.reason = result.get('reason')
        deserialized_result.params = result.get('params')
        deserialized_result.query = result.get('query')
        deserialized_result.wait_start = result.get('wait_start')
        deserialized_result.start = result.get('start')
        deserialized_result.connect_end = result.get('connect_end')
        deserialized_result.write_end = result.get('write_end')
        deserialized_result.complete = result.get('complete')
        deserialized_result.checks = result.get('checks')

        deserialized_result.type = RequestTypes.HTTP2

        return deserialized_result