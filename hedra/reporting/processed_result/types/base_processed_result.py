import uuid
from typing import Any, List, Dict, Union
from hedra.core.hooks.types.base.event import BaseEvent
from hedra.core.engines.types.common.base_result import BaseResult
from hedra.reporting.tags import Tag


class BaseProcessedResult:

    __slots__ = (
        'stage_name',
        'event_id',
        'action_id',
        'name',
        'shortname',
        'error',
        'type',
        'source',
        'checks',
        'tags',
        'stage',
        'timings',
        'time'
    )

    def __init__(
        self, 
        stage_name: str, 
        result: BaseResult
    ) -> None:

        self.event_id = str(uuid.uuid4())
        self.action_id = result.action_id

        self.name = result.name
        self.shortname = result.name
        self.error = result.error
        self.timings = {}
        self.type = result.type
        self.source = result.source
        self.checks: List[List[BaseEvent]] = []
        self.stage = stage_name

        self.time = self.timings.get('total', 0)

        self.tags = [
            Tag(
                tag.get('name'),
                tag.get('value')
            ) for tag in result.tags
        ]


    @property
    def fields(self):
        return ['id', 'name', 'stage', 'time', 'succeeded']

    @property
    def values(self):
        return [self.name, self.stage, self.time, self.success]

    @property
    def record(self):
        return {
            'id': self.event_id,
            'name': self.name,
            'stage': self.stage,
            'time': self.time,
            'succeeded': self.success
        }

    @property
    def success(self):
        return self.error is None

    def tags_to_dict(self):
        return {
            tag.name: tag.value for tag in self.tags
        }
    
    def to_dict(self) -> Dict[str, Union[str, int, float]]:
        raise NotImplementedError('Err. - Implement this method in a specific class inheriting from BaseProcessedResult')

    def serialize(self) -> str:
        raise NotImplementedError('Err. - Implement this method in a specific class inheriting from BaseProcessedResult')