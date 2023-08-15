from typing import List, Iterable, Generic, TypeVarTuple, Union
from .service import Service


P = TypeVarTuple('P')

class PluginGroup(Generic[*P]):

    def __init__(
        self,
        service_pool: List[Union[*P]]
    ) -> None:
        self._services = service_pool
        self._services_count = len(service_pool)
        self._current_idx = 0

    @property
    def one(self) -> Union[*P]:
        service: Service = self._services[self._current_idx]
        self._current_idx = (self._current_idx + 1)%self._services_count

        return service
    
    def each(self) -> Iterable[Union[*P]]:
        for service in self._services:
            yield service
    
    def at(self, idx: int) -> Union[*P]:
        return self._services[idx]
