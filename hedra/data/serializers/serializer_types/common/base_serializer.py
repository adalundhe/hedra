from hedra.core.engines.types.common.base_action import BaseAction
from hedra.data.serializers.serializer_types.common.metadata_serializer import MetadataSerializer
from typing import List, Dict, Union


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