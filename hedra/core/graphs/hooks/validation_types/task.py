from typing import List, Optional, Dict, Union, Coroutine
from pydantic import BaseModel, Field


class TaskValidator(BaseModel):
    weight: Optional[int]=Field(1, gt=0)
    order: Optional[int]=Field(1, gt=0)
    metadata: Optional[Dict[str, Union[str, int, float]]]
    checks: Optional[List[str]]
    notify: Optional[List[str]]
    listen: Optional[List[str]]


    class Config:
        arbitrary_types_allowed = True

    def __init__(
        __pydantic_self__, 
        weight: Optional[int]=1, 
        order: Optional[int]=1, 
        metadata: Optional[Dict[str, Union[str, int, float]]]={}, 
        checks: Optional[List[str]]=[],
        notify: Optional[List[str]]=[],
        listen: Optional[List[str]]=[]
    ) -> None:
        super().__init__(
            weight=weight,
            order=order,
            metadata=metadata,
            checks=checks,
            notify=notify,
            listen=listen
        )