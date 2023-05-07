from pydantic import (
    BaseModel, 
    StrictStr, 
    StrictInt
)
from typing import Optional, List


class DeformHeaderValidator(BaseModel):
    header_name: StrictStr
    deformation_length: StrictInt
    character_pool: Optional[List[StrictStr]]
    header_value: Optional[StrictStr]