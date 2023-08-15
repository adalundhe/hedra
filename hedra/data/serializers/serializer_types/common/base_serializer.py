from hedra.core.engines.types.common.base_action import BaseAction
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.data.serializers.serializer_types.common.metadata_serializer import MetadataSerializer
from typing import (
    List, 
    Dict, 
    Union, 
    Any
)


class BaseSerializer:

    def __init__(self) -> None:
        self.metadata_serializer = MetadataSerializer()

    def action_to_serializable(
        self, 
        action: BaseAction
    ) -> Dict[str, Union[str, List[str]]]:
        return {
            'action_id': action.action_id,
            'name': action.name,
            'metadata': self.metadata_serializer.serialize_metadata(
                action.metadata
            ),
        }
    
    def result_to_serializable(
        self,
        result: BaseResult
    ) -> Dict[str, Any]:
        return {
            'name': result.name,
            'error': str(result.error),
            'source': result.source,
            'user': result.user,
            'tags': result.tags,
            'type': result.type,
            'wait_start': float(result.wait_start),
            'start': float(result.start),
            'connect_end': float(result.connect_end),
            'write_end': float(result.write_end),
            'complete': float(result.complete),
            'checks': result.checks
        }