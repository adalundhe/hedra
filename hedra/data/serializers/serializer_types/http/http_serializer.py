from hedra.core.engines.types.common.timeouts import Timeouts
from hedra.core.engines.types.http.action import HTTPAction
from hedra.core.engines.types.http.client import MercuryHTTPClient
from hedra.core.engines.types.http.result import HTTPResult
from hedra.core.engines.types.common.types import RequestTypes
from hedra.data.serializers.serializer_types.common.base_serializer import BaseSerializer
from typing import List, Dict, Union, Any


class HTTPSerializer(BaseSerializer):

    def __init__(self) -> None:
        super().__init__()

    def action_to_serializable(
        self,
        action: HTTPAction
    ) -> Dict[str, Union[str, List[str]]]:
        
        serialized_action = super().action_to_serializable(action)
        return {
            **serialized_action,
            'type': RequestTypes.HTTP,
            'url': {
                'full': action.url.full,
                'ip_addr': action.url.ip_addr,
                'socket_config': action.url.socket_config,
                'has_ip_addr': action.url.has_ip_addr
            },
            'method': action.method,
            'headers': action._headers,
            'header_items': action._header_items,
            'encoded_headers': action.encoded_headers,
            'data': action.data,
            'encoded_data': action.encoded_data,
            'is_stream': action.is_stream,
            'redirects': action.redirects,
            'is_setup': action.is_setup,
            'action_args': action.action_args,
        }
    
    def deserialize_action(
        self,
        action: Dict[str, Any]
    ) -> HTTPAction:
        
        url_config = action.get('url', {})
        metadata = action.get('metadata', {})
        
        http_action = HTTPAction(
            name=action.get('name'),
            url=url_config.get('full'),
            headers=action.get('headers'),
            data=action.get('data'),
            user=metadata.get('user'),
            tags=metadata.get('tags', []),
            redirects=action.get('redirects', 3)
        )

        http_action.url.ip_addr = url_config.get('ip_addr')
        http_action.url.socket_config = url_config.get('socket_config')
        http_action.url.has_ip_addr = url_config.get('has_ip_addr')

        http_action.setup()

        return http_action
    
    def deserialize_client_config(self, client_config: Dict[str, Any]) -> MercuryHTTPClient:
        return MercuryHTTPClient(
            concurrency=client_config.get('concurrency'),
            timeouts=Timeouts(
                **client_config.get('timeouts', {})
            ),
            reset_connections=client_config.get('reset_sessions')
        )
    
    def result_to_serializable(
        self,
        result: HTTPResult
    ) -> Dict[str, Any]:
        
        serialized_result = super().result_to_serializable(result)

        encoded_headers = {
            str(k.decode()): str(v.decode()) for k, v in result.headers.items()
        }

        body: Union[str, None] = None
        if result.body:
            body = str(result.body.decode())

        return {
            **serialized_result,
            'url': result.url,
            'method': result.method,
            'path': result.path,
            'params': result.params,
            'query': result.query,
            'type': result.type,
            'headers': encoded_headers,
            'body': body,
            'tags': result.tags,
            'user': result.user,
            'error': str(result.error),
            'status': result.status,
            'reason': result.reason,
        }
    
    def deserialize_result(
        self,
        result: Dict[str, Any]
    ) -> HTTPResult:
        deserialized_result = HTTPResult(
            HTTPAction(
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

        body = result.get('body')
        if isinstance(body, str):
            body = body.encode()

        deserialized_result.body = body
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

        deserialized_result.type = RequestTypes.HTTP

        return deserialized_result
