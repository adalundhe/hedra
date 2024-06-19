from typing import Any, Dict

from pydantic import (
    BaseModel,
)


class HeaderValidator(BaseModel):
    value: Dict[str, Any]
