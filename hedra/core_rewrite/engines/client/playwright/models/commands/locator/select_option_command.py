from typing import (
    Optional,
    Sequence,
)

from playwright.async_api import ElementHandle
from pydantic import (
    BaseModel,
    StrictBool,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class SelectOptionCommand(BaseModel):
    value: Optional[StrictStr | Sequence[StrictStr]]=None
    index: Optional[StrictInt | Sequence[StrictInt]]=None
    label: Optional[StrictStr | Sequence[StrictStr]]=None
    element: Optional[ElementHandle | Sequence[ElementHandle]]=None
    no_wait_after: Optional[StrictBool]=None
    force: Optional[StrictBool]=None
    timeout: StrictInt | StrictFloat

    class Config:
        arbitrary_types_allowed=True