from typing import Optional, Pattern

from pydantic import (
    BaseModel,
)


class GetByTestIdCommand(BaseModel):
    test_id: str | Pattern[str]
    timeout: Optional[int | float]=None