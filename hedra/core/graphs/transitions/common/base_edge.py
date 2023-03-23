from __future__ import annotations
from typing import Any, Generic, TypeVar, Dict, List, Union
from hedra.core.graphs.stages.base.stage import Stage
from hedra.core.graphs.stages.types.stage_types import StageTypes


T = TypeVar('T')


class BaseEdge(Generic[T]):

    def __init__(self, source: Union[T, Stage], destination: Stage) -> None:
        self.requires = []
        self.provides = []
        
        self.from_stage_name: str = None
        self.from_stage_names: List[str] = []
        self.stages_by_type: Dict[StageTypes, Dict[str, Stage]] = {}
        self.path_lengths: Dict[str, int] = {}
        self.history = {}
        self.edge_data = {}
        self.next_history = {}
        self.visited = []
        self.valid_states = []
        self.descendants = []
        self.all_paths: Dict[str, List[str]] = {}
        self.source = source
        self.destination = destination
        self.timeout = None
        self.folded = False
        self.transition_idx = 0

        self.edges_by_name: Dict[str, Stage] = {}

        for stage_type in StageTypes:
            self.stages_by_type[stage_type] = {}

        self.skip_stage = self.source.skip

    def __getitem__(self, key: str):
        return self.history.get(key)


    def __setitem__(self, key: str, value: Any):
        self.history[key] = value
        

    async def transition(self):
        raise NotImplementedError('Err. - Please implement this method in the Edge class inheriting BaseEdge')

    def update(self, destingation: Stage):
        raise NotImplementedError('Err. - Please implement this method in the Edge class inheriting BaseEdge')

    def split(self) -> None:
        raise NotImplementedError('Err. - Please implement this method in the Edge class inheriting BaseEdge')

    def merge(self) -> None:
        raise NotImplementedError('Err. - Please implement this method in the Edge class inheriting BaseEdge')
    
    def setup(self) -> None:
        pass
        #raise NotImplementedError('Err. - Please implement this method in the Edge class inheriting BaseEdge')