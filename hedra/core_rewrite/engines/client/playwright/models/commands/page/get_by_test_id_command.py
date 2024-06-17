from typing import Pattern

from pydantic import (
    BaseModel,
    StrictFloat,
    StrictInt,
    StrictStr,
)


class GetByTestIdCommand(BaseModel):
    test_id: StrictStr | Pattern[str]
    timeout: StrictInt | StrictFloat